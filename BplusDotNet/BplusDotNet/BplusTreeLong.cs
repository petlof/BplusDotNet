using System;
using System.Collections;

// delete next
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;

namespace BplusDotNet
{
	/// <summary>
	/// Bplustree mapping fixed length strings (byte sequences) to longs (seek positions in file indexed).
	/// "Next leaf pointer" is not used since it increases the chance of file corruption on failure.
	/// All modifications are "shadowed" until a flush of all modifications succeeds.  Modifications are
	/// "hardened" when the header record is rewritten with a new root.  This design trades a few "unneeded"
	/// buffer writes for lower likelihood of file corruption.
	/// </summary>
	public class BplusTreeLong: ITreeIndex
	{
		public Stream Fromfile;
		// should be read only
		public bool DontUseCulture = false;
		public CultureInfo CultureContext;
		CompareInfo m_cmp;
		// should be read only
		public BufferFile Buffers;
		// should be read only
		public int Buffersize;
		// should be read only
		public int KeyLength;
		public long SeekStart = 0;
		public static byte[] Headerprefix = { 98, 112, 78, 98, 112 };
		// header consists of 
		// prefix | version | node size | key size | culture id | buffer number of root | buffer number of free list head
	    readonly int m_headersize = Headerprefix.Length + 1 + BufferFile.Intstorage*3 + BufferFile.Longstorage*2;
		public const byte Version = 0;
		// size of allocated key space in each node (should be a read only property)
		public int NodeSize;
		BplusNode m_root;
		long m_rootSeek; 
		long m_freeHeadSeek;
		public Hashtable FreeBuffersOnCommit = new Hashtable();
		public Hashtable FreeBuffersOnAbort = new Hashtable();
	    readonly Hashtable m_idToTerminalNode = new Hashtable();
	    readonly Hashtable m_terminalNodeToId = new Hashtable();
		int m_terminalNodeCount;
		int m_lowerTerminalNodeCount;
		int m_fifoLimit = 100;
		public static int Nullbuffernumber = -1;
		public static byte Nonleaf = 0, Leaf = 1, Free = 2;

		public BplusTreeLong(Stream fromfile, int nodeSize, int keyLength, long startSeek, int cultureId)
		{
			CultureContext = new CultureInfo(cultureId);
			m_cmp = CultureContext.CompareInfo;
			Fromfile = fromfile;
			NodeSize = nodeSize;
			SeekStart = startSeek;
			// add in key prefix overhead
			KeyLength = keyLength + BufferFile.Shortstorage;
			m_rootSeek = Nullbuffernumber;
			m_root = null;
			m_freeHeadSeek = Nullbuffernumber;
			SanityCheck();
		}
		
		public int MaxKeyLength() 
		{
			return KeyLength-BufferFile.Shortstorage;
		}
		public void Shutdown()
		{
			Fromfile.Flush();
			Fromfile.Close();
		}
		public int Compare(string left, string right) 
		{
			//System.Globalization.CompareInfo cmp = this.cultureContext.CompareInfo;
			if (CultureContext==null || DontUseCulture) 
			{
				// no culture context: use miscellaneous total ordering on unicode strings
				var i = 0;
				while (i<left.Length && i<right.Length) 
				{
					var leftOrd = Convert.ToInt32(left[i]);
					var rightOrd = Convert.ToInt32(right[i]);
					if (leftOrd<rightOrd) 
					{
						return -1;
					}
					if (leftOrd>rightOrd)
					{
						return 1;
					}
					i++;
				}
				if (left.Length<right.Length) 
				{
					return -1;
				}
				if (left.Length>right.Length) 
				{
					return 1;
				}
				return 0;
			}
			if (m_cmp==null) 
			{
				m_cmp = CultureContext.CompareInfo;
			}
			return m_cmp.Compare(left, right);
		}
		public void SanityCheck(bool strong) 
		{
			SanityCheck();
			if (strong) 
			{
				Recover(false);
				// look at all deferred deallocations -- they should not be free
				var buffer = new byte[1];
				foreach (DictionaryEntry thing in FreeBuffersOnAbort) 
				{
					var buffernumber = (long) thing.Key;
					Buffers.GetBuffer(buffernumber, buffer, 0, 1);
					if (buffer[0]==Free) 
					{
						throw new BplusTreeException("free on abort buffer already marked free "+buffernumber);
					}
				}
				foreach (DictionaryEntry thing in FreeBuffersOnCommit) 
				{
					var buffernumber = (long) thing.Key;
					Buffers.GetBuffer(buffernumber, buffer, 0, 1);
					if (buffer[0]==Free) 
					{
						throw new BplusTreeException("free on commit buffer already marked free "+buffernumber);
					}
				}
			}
		}
		public void Recover(bool correctErrors) 
		{
			var visited = new Hashtable();
			if (m_root!=null) 
			{
				// find all reachable nodes
				m_root.SanityCheck(visited);
			}
			// traverse the free list
			var freebuffernumber = m_freeHeadSeek;
			while (freebuffernumber!=Nullbuffernumber) 
			{
				if (visited.ContainsKey(freebuffernumber) ) 
				{
					throw new BplusTreeException("free buffer visited twice "+freebuffernumber);
				}
				visited[freebuffernumber] = Free;
				freebuffernumber = ParseFreeBuffer(freebuffernumber);
			}
			// find out what is missing
			var missing = new Hashtable();
			var maxbuffer = Buffers.NextBufferNumber();
			for (long i=0; i<maxbuffer; i++) 
			{
				if (!visited.ContainsKey(i)) 
				{
					missing[i] = i;
				}
			}
			// remove from missing any free-on-commit blocks
			foreach (DictionaryEntry thing in FreeBuffersOnCommit) 
			{
				var tobefreed = (long) thing.Key;
				missing.Remove(tobefreed);
			}
			// add the missing values to the free list
			if (correctErrors) 
			{
				if (missing.Count>0) 
				{
					Debug.WriteLine("correcting "+missing.Count+" unreachable buffers");
				}
				var missingL = new ArrayList();
				foreach (DictionaryEntry d in missing) 
				{
					missingL.Add(d.Key);
				}
				missingL.Sort();
				missingL.Reverse();
				foreach (var thing in missingL) 
				{
					var buffernumber = (long) thing;
					DeallocateBuffer(buffernumber);
				}
				//this.ResetBookkeeping();
			} 
			else if (missing.Count>0)
			{
				var buffers = "";
				foreach (DictionaryEntry thing in missing) 
				{
					buffers += " "+thing.Key;
				}
				throw new BplusTreeException("found "+missing.Count+" unreachable buffers."+buffers);
			}
		}
		public void SerializationCheck()  
		{
			if (m_root==null) 
			{
				throw new BplusTreeException("serialization check requires initialized root, sorry");
			}
			m_root.SerializationCheck();
		}
		void SanityCheck() 
		{
			if (NodeSize<2) 
			{
				throw new BplusTreeException("node size must be larger than 2");
			}
			if (KeyLength<5) 
			{
				throw new BplusTreeException("Key length must be larger than 5");
			}
			if (SeekStart<0) 
			{
				throw new BplusTreeException("start seek may not be negative");
			}
			// compute the buffer size
			// indicator | seek position | [ key storage | seek position ]*
			var keystorage = KeyLength + BufferFile.Shortstorage;
			Buffersize = 1 + BufferFile.Longstorage + (keystorage + BufferFile.Longstorage)*NodeSize;
		}
		public string toHtml() 
		{
			var sb = new StringBuilder();
			sb.Append("<h1>BplusTree</h1>\r\n");
			sb.Append("\r\n<br> nodesize="+NodeSize);
			sb.Append("\r\n<br> seekstart="+SeekStart);
			sb.Append("\r\n<br> rootseek="+m_rootSeek);
			sb.Append("\r\n<br> free on commit "+FreeBuffersOnCommit.Count+" ::");
			foreach (DictionaryEntry thing in FreeBuffersOnCommit) 
			{
				sb.Append(" "+thing.Key);
			}
			sb.Append("\r\n<br> Freebuffers : ");
			var freevisit = new Hashtable();
			var free = m_freeHeadSeek;
			var allfree = "freehead="+free+" :: ";
			while (free!=Nullbuffernumber) 
			{
				allfree = allfree+" "+free;
				if (freevisit.ContainsKey(free)) 
				{
					throw new BplusTreeException("cycle in freelist "+free);
				}
				freevisit[free] = free;
				free = ParseFreeBuffer(free);
			}
		    sb.Append(allfree.Length == 0 ? "empty list" : allfree);
		    foreach (DictionaryEntry thing in FreeBuffersOnCommit) 
			{
				sb.Append(" "+thing.Key);
			}
			sb.Append("\r\n<br> free on abort "+FreeBuffersOnAbort.Count+" ::");
			foreach (DictionaryEntry thing in FreeBuffersOnAbort) 
			{
				sb.Append(" "+thing.Key);
			}
			sb.Append("\r\n<br>\r\n");

			//... add more
			if (m_root==null) 
			{
				sb.Append("<br><b>NULL ROOT</b>\r\n");
			} 
			else 
			{
				m_root.AsHtml(sb);
			}
			return sb.ToString();
		}
		public BplusTreeLong(Stream fromfile, int keyLength, int nodeSize, int cultureId):
			this(fromfile, nodeSize, keyLength, 0, cultureId) 
		{
			// just start seek at 0
		}
		public static BplusTreeLong SetupFromExistingStream(Stream fromfile) 
		{
			return SetupFromExistingStream(fromfile, 0);
		}
		public static BplusTreeLong SetupFromExistingStream(Stream fromfile, long startSeek) 
		{
			var dummyId = CultureInfo.InvariantCulture.LCID;
			var result = new BplusTreeLong(fromfile, 7, 100, startSeek, dummyId); // dummy values for nodesize, keysize
			result.ReadHeader();
			result.Buffers = BufferFile.SetupFromExistingStream(fromfile, startSeek+result.m_headersize);
			if (result.Buffers.Buffersize != result.Buffersize) 
			{
				throw new BplusTreeException("inner and outer buffer sizes should match");
			}
			if (result.m_rootSeek!=Nullbuffernumber) 
			{
				result.m_root = new BplusNode(result, null, -1, true);
				result.m_root.LoadFromBuffer(result.m_rootSeek);
			}
			return result;
		}
		public static BplusTreeLong InitializeInStream(Stream fromfile, int keyLength, int nodeSize) 
		{
			var dummyId = CultureInfo.InvariantCulture.LCID;
			return InitializeInStream(fromfile, keyLength, nodeSize, dummyId);
		}
		public static BplusTreeLong InitializeInStream(Stream fromfile, int keyLength, int nodeSize, int cultureId) 
		{
			return InitializeInStream(fromfile, keyLength, nodeSize, cultureId, 0);
		}
		public static BplusTreeLong InitializeInStream(Stream fromfile, int keyLength, int nodeSize, int cultureId, long startSeek) 
		{
			if (fromfile.Length>startSeek) 
			{
				throw new BplusTreeException("can't initialize bplus tree inside written area of stream");
			}
			var result = new BplusTreeLong(fromfile, nodeSize, keyLength, startSeek, cultureId);
			result.SetHeader();
			result.Buffers = BufferFile.InitializeBufferFileInStream(fromfile, result.Buffersize, startSeek+result.m_headersize);
			return result;
		}
		public void SetFootPrintLimit(int limit) 
		{
			if (limit<5) 
			{
				throw new BplusTreeException("foot print limit less than 5 is too small");
			}
			m_fifoLimit = limit;
		}
		public void RemoveKey(string key) 
		{
			if (m_root==null) 
			{
				throw new BplusTreeKeyMissing("tree is empty: cannot delete");
			}
			bool mergeMe;
			var theroot = m_root;
			theroot.Delete(key, out mergeMe);
			// if the root is not a leaf and contains only one child (no key), reroot
			if (mergeMe && !m_root.IsLeaf && m_root.SizeInUse()==0) 
			{
				m_root = m_root.FirstChild();
				m_rootSeek = m_root.MakeRoot();
				theroot.Free();
			}
		}
		public long this[string key] 
		{
			get 
			{
				long valueFound;
				var test = ContainsKey(key, out valueFound);
				if (!test) 
				{
					throw new BplusTreeKeyMissing("no such key found: "+key);
				}
				return valueFound;
			}
			set 
			{
				if (!BplusNode.KeyOK(key, this)) 
				{
					throw new BplusTreeBadKeyValue("null or too large key cannot be inserted into tree: "+key);
				}
				var rootinit = false;
				if (m_root==null) 
				{
					// allocate root
					m_root = new BplusNode(this, null, -1, true);
					rootinit = true;
					//this.rootSeek = root.DumpToFreshBuffer();
				}
				// insert into root...
				string splitString;
				BplusNode splitNode;
				m_root.Insert(key, value, out splitString, out splitNode);
				if (splitNode!=null) 
				{
					// split of root: make a new root.
					rootinit = true;
					var oldRoot = m_root;
					m_root = BplusNode.BinaryRoot(oldRoot, splitString, splitNode, this);
				}
				if (rootinit) 
				{
					m_rootSeek = m_root.DumpToFreshBuffer();
				}
				// check size in memory
				ShrinkFootprint();
			}
		}
		public string FirstKey() 
		{
			string result = null;
			if (m_root!=null) 
			{
				// empty string is smallest possible tree
				if (ContainsKey("")) 
				{
					result = "";
				} 
				else 
				{
					return m_root.FindNextKey("");
				}
				ShrinkFootprint();
			}
			return result;
		}
		public string NextKey(string afterThisKey) 
		{
			if (afterThisKey==null) 
			{
				throw new BplusTreeBadKeyValue("cannot search for null string");
			}
			var result = m_root.FindNextKey(afterThisKey);
			ShrinkFootprint();
			return result;
		}
		public bool ContainsKey(string key) 
		{
			long valueFound;
			return ContainsKey(key, out valueFound);
		} 
		public bool ContainsKey(string key, out long valueFound) 
		{
			if (key==null)
			{
				throw new BplusTreeBadKeyValue("cannot search for null string");
			}
			var result = false;
			valueFound = 0;
			if (m_root!=null) 
			{
				result = m_root.FindMatch(key, out valueFound);
			}
			ShrinkFootprint();
			return result;
		}
		public long Get(string key, long defaultValue) 
		{
			var result = defaultValue;
			long valueFound;
			if (ContainsKey(key, out valueFound))
			{
				result = valueFound;
			}
			return result;
		}
		public void Set(string key, object map) 
		{
			if (!(map is long)) 
			{
				throw new BplusTreeBadKeyValue("only longs may be used as values in a BplusTreeLong: "+map);
			}
			this[key] = (long) map;
		}
		public object Get(string key, object defaultValue) 
		{
			long valueFound;
			if (ContainsKey(key, out valueFound)) 
			{
				return valueFound;
			}
			return defaultValue;
		}
		/// <summary>
		/// Store off any changed buffers, clear the fifo, free invalid buffers
		/// </summary>
		public void Commit() 
		{
			// store all modifications
			if (m_root!=null) 
			{
				m_rootSeek = m_root.Invalidate(false);
			}
			Fromfile.Flush();
			// commit the new root
			SetHeader();
			Fromfile.Flush();
			// at this point the changes are committed, but some space is unreachable.
			// now free all unfreed buffers no longer in use
			var toFree = new ArrayList();
			foreach (DictionaryEntry d in FreeBuffersOnCommit) 
			{
				toFree.Add(d.Key);
			}
			toFree.Sort();
			toFree.Reverse();
			foreach (var thing in toFree) 
			{
				var buffernumber = (long) thing;
				DeallocateBuffer(buffernumber);
			}
			// store the free list head
			SetHeader();
			Fromfile.Flush();
			ResetBookkeeping();
		}
		/// <summary>
		/// Forget all changes since last commit
		/// </summary>
		public void Abort() 
		{
			// deallocate allocated blocks
			var toFree = new ArrayList();
			foreach (DictionaryEntry d in FreeBuffersOnAbort) 
			{
				toFree.Add(d.Key);
			}
			toFree.Sort();
			toFree.Reverse();
			foreach (var thing in toFree) 
			{
				var buffernumber = (long) thing;
				DeallocateBuffer(buffernumber);
			}
			var freehead = m_freeHeadSeek;
			// reread the header (except for freelist head)
			ReadHeader();
			// restore the root
			if (m_rootSeek==Nullbuffernumber) 
			{
				m_root = null; // nothing was committed
			} 
			else 
			{
				m_root.LoadFromBuffer(m_rootSeek);
			}
			ResetBookkeeping();
			m_freeHeadSeek = freehead;
			SetHeader(); // store new freelist head
			Fromfile.Flush();
		}
		void ResetBookkeeping() 
		{
			FreeBuffersOnCommit.Clear();
			FreeBuffersOnAbort.Clear();
			m_idToTerminalNode.Clear();
			m_terminalNodeToId.Clear();
		}
		public long AllocateBuffer() 
		{
			long allocated;
			if (m_freeHeadSeek==Nullbuffernumber) 
			{
				// should be written immediately after allocation
				allocated = Buffers.NextBufferNumber();
				//System.Diagnostics.Debug.WriteLine("<br> allocating fresh buffer "+allocated);
				return allocated;
			}
			// get the free head data
			allocated = m_freeHeadSeek;
			m_freeHeadSeek = ParseFreeBuffer(allocated);
			//System.Diagnostics.Debug.WriteLine("<br> recycling free buffer "+allocated);
			return allocated;
		}
		long ParseFreeBuffer(long buffernumber) 
		{
			const int freesize = 1+BufferFile.Longstorage;
			var buffer = new byte[freesize];
			Buffers.GetBuffer(buffernumber, buffer, 0, freesize);
			if (buffer[0]!=Free) 
			{
				throw new BplusTreeException("free buffer not marked free");
			}
			var result = BufferFile.RetrieveLong(buffer, 1);
			return result;
		}
		public void DeallocateBuffer(long buffernumber) 
		{
			//System.Diagnostics.Debug.WriteLine("<br> deallocating "+buffernumber);
			const int freesize = 1+BufferFile.Longstorage;
			var buffer = new byte[freesize];
			// it better not already be marked free
			Buffers.GetBuffer(buffernumber, buffer, 0, 1);
			if (buffer[0]==Free) 
			{
				throw new BplusTreeException("attempt to re-free free buffer not allowed");
			}
			buffer[0] = Free;
			BufferFile.Store(m_freeHeadSeek, buffer, 1);
			Buffers.SetBuffer(buffernumber, buffer, 0, freesize);
			m_freeHeadSeek = buffernumber;
		}
		void SetHeader() 
		{
			var header = MakeHeader();
			Fromfile.Seek(SeekStart, SeekOrigin.Begin);
			Fromfile.Write(header, 0, header.Length);
		}
		public void RecordTerminalNode(BplusNode terminalNode) 
		{
			if (terminalNode==m_root) 
			{
				return; // never record the root node
			}
			if (m_terminalNodeToId.ContainsKey(terminalNode) )
			{
				return; // don't record it again
			}
			var id = m_terminalNodeCount;
			m_terminalNodeCount++;
			m_terminalNodeToId[terminalNode] = id;
			m_idToTerminalNode[id] = terminalNode;
		}
		public void ForgetTerminalNode(BplusNode nonterminalNode) 
		{
			if (!m_terminalNodeToId.ContainsKey(nonterminalNode)) 
			{
				// silently ignore (?)
				return;
			}
			var id = (int) m_terminalNodeToId[nonterminalNode];
			if (id == m_lowerTerminalNodeCount) 
			{
				m_lowerTerminalNodeCount++;
			}
			m_idToTerminalNode.Remove(id);
			m_terminalNodeToId.Remove(nonterminalNode);
		}
		public void ShrinkFootprint() 
		{
			InvalidateTerminalNodes(m_fifoLimit);
		}
		public void InvalidateTerminalNodes(int toLimit) 
		{
			while (m_terminalNodeToId.Count>toLimit) 
			{
				// choose oldest nonterminal and deallocate it
				while (!m_idToTerminalNode.ContainsKey(m_lowerTerminalNodeCount)) 
				{
					m_lowerTerminalNodeCount++; // since most nodes are terminal this should usually be a short walk
					//System.Diagnostics.Debug.WriteLine("<BR>WALKING "+this.LowerTerminalNodeCount);
					//System.Console.WriteLine("<BR>WALKING "+this.LowerTerminalNodeCount);
					if (m_lowerTerminalNodeCount>m_terminalNodeCount) 
					{
						throw new BplusTreeException("internal error counting nodes, lower limit went too large");
					}
				}
				//System.Console.WriteLine("<br> done walking");
				var id = m_lowerTerminalNodeCount;
				var victim = (BplusNode) m_idToTerminalNode[id];
				//System.Diagnostics.Debug.WriteLine("\r\n<br>selecting "+victim.myBufferNumber+" for deallocation from fifo");
				m_idToTerminalNode.Remove(id);
				m_terminalNodeToId.Remove(victim);
				if (victim.MyBufferNumber!=Nullbuffernumber) 
				{
					victim.Invalidate(true);
				}
			}
		}
		void ReadHeader() 
		{
			// prefix | version | node size | key size | culture id | buffer number of root | buffer number of free list head
			var header = new byte[m_headersize];
			Fromfile.Seek(SeekStart, SeekOrigin.Begin);
			Fromfile.Read(header, 0, m_headersize);
			var index = 0;
			// check prefix
			foreach (var b in Headerprefix) 
			{
				if (header[index]!=b) 
				{
					throw new BufferFileException("invalid header prefix");
				}
				index++;
			}
			// skip version (for now)
			index++;
			NodeSize = BufferFile.Retrieve(header, index);
			index+= BufferFile.Intstorage;
			KeyLength = BufferFile.Retrieve(header, index);
			index+= BufferFile.Intstorage;
			var cultureId = BufferFile.Retrieve(header, index);
			CultureContext = new CultureInfo(cultureId);
			index+= BufferFile.Intstorage;
			m_rootSeek = BufferFile.RetrieveLong(header, index);
			index+= BufferFile.Longstorage;
			m_freeHeadSeek = BufferFile.RetrieveLong(header, index);
			SanityCheck();
			//this.header = header;
		}
		public byte[] MakeHeader() 
		{
			// prefix | version | node size | key size | culture id | buffer number of root | buffer number of free list head
			var result = new byte[m_headersize];
			Headerprefix.CopyTo(result, 0);
			result[Headerprefix.Length] = Version;
			var index = Headerprefix.Length+1;
			BufferFile.Store(NodeSize, result, index);
			index+= BufferFile.Intstorage;
			BufferFile.Store(KeyLength, result, index);
			index+= BufferFile.Intstorage;
		    BufferFile.Store(CultureContext != null ? CultureContext.LCID : CultureInfo.InvariantCulture.LCID, result, index);
		    index+= BufferFile.Intstorage;
			BufferFile.Store(m_rootSeek, result, index);
			index+= BufferFile.Longstorage;
			BufferFile.Store(m_freeHeadSeek, result, index);
			return result;
		}
	}
	public class BplusNode 
	{
		public bool IsLeaf = true;
		// the maximum number of children to each node.
		int m_size;
		// false if the node is no longer active and should not be used.
		//bool isValid = true;
		// true if the materialized node needs to be persisted.
		bool m_dirty = true;
		// if non-root reference to the parent node containing this node
		BplusNode m_parent;
		// tree containing this node
		BplusTreeLong m_owner;
		// buffer number of this node
		public long MyBufferNumber = BplusTreeLong.Nullbuffernumber;
		// number of children used by this node
		//int NumberOfValidKids = 0;
		long[] m_childBufferNumbers;
		string[] m_childKeys;
		BplusNode[] m_materializedChildNodes;
		int m_indexInParent = -1;

	    /// <summary>
	    /// Create a new BplusNode and install in parent if parent is not null.
	    /// </summary>
	    /// <param name="owner">tree containing the node</param>
	    /// <param name="parent">parent node (if provided)</param>
	    /// <param name="indexInParent">location in parent if provided</param>
	    /// <param name="isLeaf"></param>
	    public BplusNode(BplusTreeLong owner, BplusNode parent, int indexInParent, bool isLeaf) 
		{
			IsLeaf = isLeaf;
			m_owner = owner;
			m_parent = parent;
			m_size = owner.NodeSize;
			//this.isValid = true;
			m_dirty = true;
			//			this.ChildBufferNumbers = new long[this.Size+1];
			//			this.ChildKeys = new string[this.Size];
			//			this.MaterializedChildNodes = new BplusNode[this.Size+1];
			Clear();
			if (parent!=null && indexInParent>=0) 
			{
				if (indexInParent>m_size) 
				{
					throw new BplusTreeException("parent index too large");
				}
				// key info, etc, set elsewhere
				m_parent.m_materializedChildNodes[indexInParent] = this;
				MyBufferNumber = m_parent.m_childBufferNumbers[indexInParent];
				m_indexInParent = indexInParent;
			}
		}
		public BplusNode FirstChild() 
		{
			var result = MaterializeNodeAtIndex(0);
			if (result==null) 
			{
				throw new BplusTreeException("no first child");
			}
			return result;
		}
		public long MakeRoot() 
		{
			m_parent = null;
			m_indexInParent = -1;
			if (MyBufferNumber==BplusTreeLong.Nullbuffernumber) 
			{
				throw new BplusTreeException("no root seek allocated to new root");
			}
			return MyBufferNumber;
		}
		public void Free() 
		{
			if (MyBufferNumber!=BplusTreeLong.Nullbuffernumber) 
			{
				if (m_owner.FreeBuffersOnAbort.ContainsKey(MyBufferNumber)) 
				{
					// free it now
					m_owner.FreeBuffersOnAbort.Remove(MyBufferNumber);
					m_owner.DeallocateBuffer(MyBufferNumber);
				} 
				else 
				{
					// free on commit
					//this.owner.FreeBuffersOnCommit.Add(this.myBufferNumber);
					m_owner.FreeBuffersOnCommit[MyBufferNumber] = MyBufferNumber;
				}
			}
			MyBufferNumber = BplusTreeLong.Nullbuffernumber; // don't do it twice...
		}
		public void SerializationCheck() 
		{ 
			var a = new BplusNode(m_owner, null, -1, false);
			for (var i=0; i<m_size; i++) 
			{
				var j = i*0xf0f0f0f0f0f0f01;
				a.m_childBufferNumbers[i] = j;
				a.m_childKeys[i] = "k"+i;
			}
			a.m_childBufferNumbers[m_size] = 7;
			a.TestRebuffer();
			a.IsLeaf = true;
			for (var i=0; i<m_size; i++) 
			{
				var j = -i*0x3e3e3e3e3e3e666;
				a.m_childBufferNumbers[i] = j;
				a.m_childKeys[i] = "key"+i;
			}
			a.m_childBufferNumbers[m_size] = -9097;
			a.TestRebuffer();
		}
		void TestRebuffer() 
		{
			var il = IsLeaf;
			var ns = m_childBufferNumbers;
			var ks = m_childKeys;
			var buffer = new byte[m_owner.Buffersize];
			Dump(buffer);
			Clear();
			Load(buffer);
			for (var i=0; i<m_size; i++) 
			{
				if (m_childBufferNumbers[i]!=ns[i]) 
				{
					throw new BplusTreeException("didn't get back buffernumber "+i+" got "+m_childBufferNumbers[i]+" not "+ns[i]);
				}
				if (!m_childKeys[i].Equals(ks[i])) 
				{
					throw new BplusTreeException("didn't get back key "+i+" got "+m_childKeys[i]+" not "+ks[i]);
				}
			}
			if (m_childBufferNumbers[m_size]!=ns[m_size]) 
			{
				throw new BplusTreeException("didn't get back buffernumber "+m_size+" got "+m_childBufferNumbers[m_size]+" not "+ns[m_size]);
			}
			if (IsLeaf!=il) 
			{
				throw new BplusTreeException("isLeaf should be "+il+" got "+IsLeaf);
			}
		}
		public string SanityCheck(Hashtable visited) 
		{
			string result;
			if (visited==null) 
			{
				visited = new Hashtable();
			}
			if (visited.ContainsKey(this)) 
			{
				throw new BplusTreeException("node visited twice "+MyBufferNumber);
			}
			visited[this] = MyBufferNumber;
			if (MyBufferNumber!=BplusTreeLong.Nullbuffernumber) 
			{
				if (visited.ContainsKey(MyBufferNumber))
				{
					throw new BplusTreeException("buffer number seen twice "+MyBufferNumber);
				}
				visited[MyBufferNumber] = this;
			}
			if (m_parent!=null) 
			{
				if (m_parent.IsLeaf) 
				{
					throw new BplusTreeException("parent is leaf");
				}
				m_parent.MaterializeNodeAtIndex(m_indexInParent);
				if (m_parent.m_materializedChildNodes[m_indexInParent]!=this) 
				{
					throw new BplusTreeException("incorrect index in parent");
				}
				// since not at root there should be at least size/2 keys
				var limit = m_size/2;
				if (IsLeaf) 
				{
					limit--;
				}
				for (var i=0; i<limit; i++) 
				{
					if (m_childKeys[i]==null) 
					{
						throw new BplusTreeException("null child in first half");
					}
				}
			}
			result = m_childKeys[0]; // for leaf
			if (!IsLeaf) 
			{
				MaterializeNodeAtIndex(0);
				result = m_materializedChildNodes[0].SanityCheck(visited);
				for (var i=0; i<m_size; i++) 
				{
					if (m_childKeys[i]==null) 
					{
						break;
					}
					MaterializeNodeAtIndex(i+1);
					var least = m_materializedChildNodes[i+1].SanityCheck(visited);
					if (least==null) 
					{
						throw new BplusTreeException("null least in child doesn't match node entry "+m_childKeys[i]);
					}
					if (!least.Equals(m_childKeys[i])) 
					{
						throw new BplusTreeException("least in child "+least+" doesn't match node entry "+m_childKeys[i]);
					}
				}
			}
			// look for duplicate keys
			var lastkey = m_childKeys[0];
			for (var i=1; i<m_size; i++) 
			{
				if (m_childKeys[i]==null) 
				{
					break;
				}
				if (lastkey.Equals(m_childKeys[i]) ) 
				{
					throw new BplusTreeException("duplicate key in node "+lastkey);
				}
				lastkey = m_childKeys[i];
			}
			return result;
		}
		void Destroy() 
		{
			// make sure the structure is useless, it should no longer be used.
			m_owner = null;
			m_parent = null;
			m_size = -100;
			m_childBufferNumbers = null;
			m_childKeys = null;
			m_materializedChildNodes = null;
			MyBufferNumber = BplusTreeLong.Nullbuffernumber;
			m_indexInParent = -100;
			m_dirty = false;
		}
		public int SizeInUse() 
		{
			var result = 0;
			for (var i=0; i<m_size; i++) 
			{
				if (m_childKeys[i]==null) 
				{
					break;
				}
				result++;
			}
			return result;
		}
		public static BplusNode BinaryRoot(BplusNode leftNode, string key, BplusNode rightNode, BplusTreeLong owner) 
		{
			var newRoot = new BplusNode(owner, null, -1, false);
			//newRoot.Clear(); // redundant
			newRoot.m_childKeys[0] = key;
			leftNode.Reparent(newRoot, 0);
			rightNode.Reparent(newRoot, 1);
			// new root is stored elsewhere
			return newRoot;
		}
		void Reparent(BplusNode newParent, int parentIndex) 
		{
			// keys and existing parent structure must be updated elsewhere.
			m_parent = newParent;
			m_indexInParent = parentIndex;
			newParent.m_childBufferNumbers[parentIndex] = MyBufferNumber;
			newParent.m_materializedChildNodes[parentIndex] = this;
			// parent is no longer terminal
			m_owner.ForgetTerminalNode(m_parent);
		}
		void Clear() 
		{
			m_childBufferNumbers = new long[m_size+1];
			m_childKeys = new string[m_size];
			m_materializedChildNodes = new BplusNode[m_size+1];
			for (var i=0; i<m_size; i++) 
			{
				m_childBufferNumbers[i] = BplusTreeLong.Nullbuffernumber;
				m_materializedChildNodes[i] = null;
				m_childKeys[i] = null;
			}
			m_childBufferNumbers[m_size] = BplusTreeLong.Nullbuffernumber;
			m_materializedChildNodes[m_size] = null;
			// this is now a terminal node
			m_owner.RecordTerminalNode(this);
		}
		/// <summary>
		/// Find first index in self associated with a key same or greater than CompareKey
		/// </summary>
		/// <param name="compareKey">CompareKey</param>
		/// <param name="lookPastOnly">if true and this is a leaf then look for a greater value</param>
		/// <returns>lowest index of same or greater key or this.Size if no greater key.</returns>
		int FindAtOrNextPosition(string compareKey, bool lookPastOnly) 
		{
			var insertposition = 0;
			//System.Globalization.CultureInfo culture = this.owner.cultureContext;
			//System.Globalization.CompareInfo cmp = culture.CompareInfo;
			if (IsLeaf && !lookPastOnly) 
			{
				// look for exact match or greater or null
				while (insertposition<m_size && m_childKeys[insertposition]!=null &&
					//cmp.Compare(this.ChildKeys[insertposition], CompareKey)<0) 
					m_owner.Compare(m_childKeys[insertposition], compareKey)<0)
				{
					insertposition++;
				}
			} 
			else 
			{
				// look for greater or null only
				while (insertposition<m_size && m_childKeys[insertposition]!=null &&
					m_owner.Compare(m_childKeys[insertposition], compareKey)<=0) 
				{
					insertposition++;
				}
			}
			return insertposition;
		}
		/// <summary>
		/// Find the first key below atIndex, or if no such node traverse to the next key to the right.
		/// If no such key exists, return nulls.
		/// </summary>
		/// <param name="atIndex">where to look in this node</param>
		/// <param name="foundInLeaf">leaf where found</param>
		/// <param name="keyFound">key value found</param>
		void TraverseToFollowingKey(int atIndex, out BplusNode foundInLeaf, out string keyFound) 
		{
			foundInLeaf = null;
			keyFound = null;
			bool lookInParent;
			if (IsLeaf) 
			{
				lookInParent = (atIndex>=m_size) || (m_childKeys[atIndex]==null);
			} 
			else 
			{
				lookInParent = (atIndex>m_size) ||
					(atIndex>0 && m_childKeys[atIndex-1]==null);
			}
			if (lookInParent) 
			{
				// if it's anywhere it's in the next child of parent
				if (m_parent!=null && m_indexInParent>=0) 
				{
					m_parent.TraverseToFollowingKey(m_indexInParent+1, out foundInLeaf, out keyFound);
					return;
				} 
				else 
				{
					return; // no such following key
				}
			}
			if (IsLeaf) 
			{
				// leaf, we found it.
				foundInLeaf = this;
				keyFound = m_childKeys[atIndex];
			} 
			else 
			{
				// nonleaf, look in child (if there is one)
				if (atIndex==0 || m_childKeys[atIndex-1]!=null) 
				{
					var thechild = MaterializeNodeAtIndex(atIndex);
					thechild.TraverseToFollowingKey(0, out foundInLeaf, out keyFound);
				}
			}
		}
		public bool FindMatch(string compareKey, out long valueFound) 
		{
			valueFound = 0; // dummy value on failure
			BplusNode leaf;
			var position = FindAtOrNextPositionInLeaf(compareKey, out leaf, false);
			if (position<leaf.m_size) 
			{
				var key = leaf.m_childKeys[position];
				if ((key!=null) && m_owner.Compare(key, compareKey)==0) //(key.Equals(CompareKey)
				{
					valueFound = leaf.m_childBufferNumbers[position];
					return true;
				}
			}
			return false;
		}
		public string FindNextKey(string compareKey) 
		{
			string result;
			BplusNode leaf;
			var position = FindAtOrNextPositionInLeaf(compareKey, out leaf, true);
			if (position>=leaf.m_size || leaf.m_childKeys[position]==null) 
			{
				// try to traverse to the right.
				BplusNode newleaf;
				leaf.TraverseToFollowingKey(leaf.m_size, out newleaf, out result);
			} 
			else 
			{
				result = leaf.m_childKeys[position];
			}
			return result;
		}
		/// <summary>
		/// Find near-index of comparekey in leaf under this node. 
		/// </summary>
		/// <param name="compareKey">the key to look for</param>
		/// <param name="inLeaf">the leaf where found</param>
		/// <param name="lookPastOnly">If true then only look for a greater value, not an exact match.</param>
		/// <returns>index of match in leaf</returns>
		int FindAtOrNextPositionInLeaf(string compareKey, out BplusNode inLeaf, bool lookPastOnly) 
		{
			var myposition = FindAtOrNextPosition(compareKey, lookPastOnly);
			if (IsLeaf) 
			{
				inLeaf = this;
				return myposition;
			}
			var childBufferNumber = m_childBufferNumbers[myposition];
			if (childBufferNumber==BplusTreeLong.Nullbuffernumber) 
			{
				throw new BplusTreeException("can't search null subtree");
			}
			var child = MaterializeNodeAtIndex(myposition);
			return child.FindAtOrNextPositionInLeaf(compareKey, out inLeaf, lookPastOnly);
		}
		BplusNode MaterializeNodeAtIndex(int myposition) 
		{
			if (IsLeaf) 
			{
				throw new BplusTreeException("cannot materialize child for leaf");
			}
			var childBufferNumber = m_childBufferNumbers[myposition];
			if (childBufferNumber==BplusTreeLong.Nullbuffernumber) 
			{
				throw new BplusTreeException("can't search null subtree at position "+myposition+" in "+MyBufferNumber);
			}
			// is it already materialized?
			var result = m_materializedChildNodes[myposition];
			if (result!=null) 
			{
				return result;
			}
			// otherwise read it in...
			result = new BplusNode(m_owner, this, myposition, true); // dummy isLeaf value
			result.LoadFromBuffer(childBufferNumber);
			m_materializedChildNodes[myposition] = result;
			// no longer terminal
			m_owner.ForgetTerminalNode(this);
			return result;
		}
		public void LoadFromBuffer(long bufferNumber) 
		{
			// freelist bookkeeping done elsewhere
		    if (m_parent!=null) 
			{
			}
			//System.Diagnostics.Debug.WriteLine("\r\n<br> loading "+this.indexInParent+" from "+bufferNumber+" for "+parentinfo);
			var rawdata = new byte[m_owner.Buffersize];
			m_owner.Buffers.GetBuffer(bufferNumber, rawdata, 0, rawdata.Length);
			Load(rawdata);
			m_dirty = false;
			MyBufferNumber = bufferNumber;
			// it's terminal until a child is materialized
			m_owner.RecordTerminalNode(this);
		}
		public long DumpToFreshBuffer() 
		{
			var oldbuffernumber = MyBufferNumber;
			var freshBufferNumber = m_owner.AllocateBuffer();
			//System.Diagnostics.Debug.WriteLine("\r\n<br> dumping "+this.indexInParent+" from "+oldbuffernumber+" to "+freshBufferNumber);
			DumpToBuffer(freshBufferNumber);
			if (oldbuffernumber!=BplusTreeLong.Nullbuffernumber) 
			{
				//this.owner.FreeBuffersOnCommit.Add(oldbuffernumber);
				if (m_owner.FreeBuffersOnAbort.ContainsKey(oldbuffernumber)) 
				{
					// free it now
					m_owner.FreeBuffersOnAbort.Remove(oldbuffernumber);
					m_owner.DeallocateBuffer(oldbuffernumber);
				} 
				else 
				{
					// free on commit
					m_owner.FreeBuffersOnCommit[oldbuffernumber] = oldbuffernumber;
				}
			}
			//this.owner.FreeBuffersOnAbort.Add(freshBufferNumber);
			m_owner.FreeBuffersOnAbort[freshBufferNumber] = freshBufferNumber;
			return freshBufferNumber;
		}
		void DumpToBuffer(long buffernumber) 
		{
			var rawdata = new byte[m_owner.Buffersize];
			Dump(rawdata);
			m_owner.Buffers.SetBuffer(buffernumber, rawdata, 0, rawdata.Length);
			m_dirty = false;
			MyBufferNumber = buffernumber;
			if (m_parent!=null && m_indexInParent>=0 &&
				m_parent.m_childBufferNumbers[m_indexInParent]!=buffernumber) 
			{
				if (m_parent.m_materializedChildNodes[m_indexInParent]!=this) 
				{
					throw new BplusTreeException("invalid parent connection "+m_parent.MyBufferNumber+" at "+m_indexInParent);
				}
				m_parent.m_childBufferNumbers[m_indexInParent] = buffernumber;
				m_parent.Soil();
			}
		}
		void ReParentAllChildren() 
		{
			for (var i=0; i<=m_size; i++) 
			{
				var thisnode = m_materializedChildNodes[i];
				if (thisnode!=null) 
				{
					thisnode.Reparent(this, i);
				}
			}
		}
		/// <summary>
		/// Delete entry for key
		/// </summary>
		/// <param name="key">key to delete</param>
		/// <param name="mergeMe">true if the node is less than half full after deletion</param>
		/// <returns>null unless the smallest key under this node has changed in which case it returns the smallest key.</returns>
		public string Delete(string key, out bool mergeMe) 
		{
			mergeMe = false; // assumption
			string result = null;
			if (IsLeaf) 
			{
				return DeleteLeaf(key, out mergeMe);
			}
			var deleteposition = FindAtOrNextPosition(key, false);
			var deleteBufferNumber = m_childBufferNumbers[deleteposition];
			if (deleteBufferNumber==BplusTreeLong.Nullbuffernumber) 
			{
				throw new BplusTreeException("key not followed by buffer number in non-leaf (del)");
			}
			// del in subtree
			var deleteChild = MaterializeNodeAtIndex(deleteposition);
			bool mergeKid;
			var delresult = deleteChild.Delete(key, out mergeKid);
			// delete succeeded... now fix up the child node if needed.
			Soil(); // redundant ?
			// bizarre special case for 2-3  or 3-4 trees -- empty leaf
			if (delresult!=null && m_owner.Compare(delresult, key)==0) // delresult.Equals(key)
			{
				if (m_size>3) 
				{
					throw new BplusTreeException("assertion error: delete returned delete key for too large node size: "+m_size);
				}
				// junk this leaf and shift everything over
				if (deleteposition==0) 
				{
					result = m_childKeys[deleteposition];
				} 
				else if (deleteposition==m_size) 
				{
					m_childKeys[deleteposition-1] = null;
				}
				else
				{
					m_childKeys[deleteposition-1] = m_childKeys[deleteposition];
				}
				if (result!=null && m_owner.Compare(result, key)==0) // result.Equals(key)
				{
					// I'm not sure this ever happens
					MaterializeNodeAtIndex(1);
					result = m_materializedChildNodes[1].LeastKey();
				}
				deleteChild.Free();
				for (var i=deleteposition; i<m_size-1; i++) 
				{
					m_childKeys[i] = m_childKeys[i+1];
					m_materializedChildNodes[i] = m_materializedChildNodes[i+1];
					m_childBufferNumbers[i] = m_childBufferNumbers[i+1];
				}
				m_childKeys[m_size-1] = null;
				if (deleteposition<m_size) 
				{
					m_materializedChildNodes[m_size-1] = m_materializedChildNodes[m_size];
					m_childBufferNumbers[m_size-1] = m_childBufferNumbers[m_size];
				}
				m_materializedChildNodes[m_size] = null;
				m_childBufferNumbers[m_size] = BplusTreeLong.Nullbuffernumber;
				mergeMe = (SizeInUse()<m_size/2);
				ReParentAllChildren();
				return result;
			}
			if (deleteposition==0) 
			{
				// smallest key may have changed.
				result = delresult;
			}
				// update key array if needed
			else if (delresult!=null && deleteposition>0) 
			{
				if (m_owner.Compare(delresult,key)!=0) // !delresult.Equals(key)
				{
					m_childKeys[deleteposition-1] = delresult;
				} 
			}
			// if the child needs merging... do it
			if (mergeKid) 
			{
				int leftindex, rightindex;
				BplusNode leftNode;
				BplusNode rightNode;
				string keyBetween;
				if (deleteposition==0) 
				{
					// merge with next
					leftindex = deleteposition;
					rightindex = deleteposition+1;
					leftNode = deleteChild;
					//keyBetween = this.ChildKeys[deleteposition];
					rightNode = MaterializeNodeAtIndex(rightindex);
				} 
				else 
				{
					// merge with previous
					leftindex = deleteposition-1;
					rightindex = deleteposition;
					leftNode = MaterializeNodeAtIndex(leftindex);
					//keyBetween = this.ChildKeys[deleteBufferNumber-1];
					rightNode = deleteChild;
				}
				keyBetween = m_childKeys[leftindex];
				string rightLeastKey;
				bool deleteRight;
				Merge(leftNode, keyBetween, rightNode, out rightLeastKey, out deleteRight);
				// delete the right node if needed.
				if (deleteRight) 
				{
					for (var i=rightindex; i<m_size; i++) 
					{
						m_childKeys[i-1] = m_childKeys[i];
						m_childBufferNumbers[i] = m_childBufferNumbers[i+1];
						m_materializedChildNodes[i] = m_materializedChildNodes[i+1];
					}
					m_childKeys[m_size-1] = null;
					m_materializedChildNodes[m_size] = null;
					m_childBufferNumbers[m_size] = BplusTreeLong.Nullbuffernumber;
					ReParentAllChildren();
					rightNode.Free();
					// does this node need merging?
					if (SizeInUse()<m_size/2) 
					{
						mergeMe = true;
					}
				} 
				else 
				{
					// update the key entry
					m_childKeys[rightindex-1] = rightLeastKey;
				}
			}
			return result;
		}
		string LeastKey() 
		{
			string result;
			if (IsLeaf) 
			{
				result = m_childKeys[0];
			} 
			else 
			{
				MaterializeNodeAtIndex(0);
				result = m_materializedChildNodes[0].LeastKey();
			}
			if (result==null) 
			{
				throw new BplusTreeException("no key found");
			}
			return result;
		}
		public static void Merge(BplusNode left, string keyBetween, BplusNode right, out string rightLeastKey, 
			out bool deleteRight) 
		{
			//System.Diagnostics.Debug.WriteLine("\r\n<br> merging "+right.myBufferNumber+" ("+KeyBetween+") "+left.myBufferNumber);
			//System.Diagnostics.Debug.WriteLine(left.owner.toHtml());
			rightLeastKey = null; // only if DeleteRight
			if (left.IsLeaf || right.IsLeaf) 
			{
				if (!(left.IsLeaf&&right.IsLeaf)) 
				{
					throw new BplusTreeException("can't merge leaf with non-leaf");
				}
				MergeLeaves(left, right, out deleteRight);
				rightLeastKey = right.m_childKeys[0];
				return;
			}
			// merge non-leaves
			deleteRight = false;
			var allkeys = new string[left.m_size*2+1];
			var allseeks = new long[left.m_size*2+2];
			var allMaterialized = new BplusNode[left.m_size*2+2];
			if (left.m_childBufferNumbers[0]==BplusTreeLong.Nullbuffernumber ||
				right.m_childBufferNumbers[0]==BplusTreeLong.Nullbuffernumber) 
			{
				throw new BplusTreeException("cannot merge empty non-leaf with non-leaf");
			}
			var index = 0;
			allseeks[0] = left.m_childBufferNumbers[0];
			allMaterialized[0] = left.m_materializedChildNodes[0];
			for (var i=0; i<left.m_size; i++) 
			{
				if (left.m_childKeys[i]==null) 
				{
					break;
				}
				allkeys[index] = left.m_childKeys[i];
				allseeks[index+1] = left.m_childBufferNumbers[i+1];
				allMaterialized[index+1] = left.m_materializedChildNodes[i+1];
				index++;
			}
			allkeys[index] = keyBetween;
			index++;
			allseeks[index] = right.m_childBufferNumbers[0];
			allMaterialized[index] = right.m_materializedChildNodes[0];
			var rightcount = 0;
			for (var i=0; i<right.m_size; i++) 
			{
				if (right.m_childKeys[i]==null) 
				{
					break;
				}
				allkeys[index] = right.m_childKeys[i];
				allseeks[index+1] = right.m_childBufferNumbers[i+1];
				allMaterialized[index+1] = right.m_materializedChildNodes[i+1];
				index++;
				rightcount++;
			}
			if (index<=left.m_size) 
			{
				// it will all fit in one node
				//System.Diagnostics.Debug.WriteLine("deciding to forget "+right.myBufferNumber+" into "+left.myBufferNumber);
				deleteRight = true;
				for (var i=0; i<index; i++) 
				{
					left.m_childKeys[i] = allkeys[i];
					left.m_childBufferNumbers[i] = allseeks[i];
					left.m_materializedChildNodes[i] = allMaterialized[i];
				}
				left.m_childBufferNumbers[index] = allseeks[index];
				left.m_materializedChildNodes[index] = allMaterialized[index];
				left.ReParentAllChildren();
				left.Soil();
				right.Free();
				return;
			}
			// otherwise split the content between the nodes
			left.Clear();
			right.Clear();
			left.Soil();
			right.Soil();
			var leftcontent = index/2;
			var rightcontent = index-leftcontent-1;
			rightLeastKey = allkeys[leftcontent];
			var outputindex = 0;
			for (var i=0; i<leftcontent; i++) 
			{
				left.m_childKeys[i] = allkeys[outputindex];
				left.m_childBufferNumbers[i] = allseeks[outputindex];
				left.m_materializedChildNodes[i] = allMaterialized[outputindex];
				outputindex++;
			}
			rightLeastKey = allkeys[outputindex];
			left.m_childBufferNumbers[outputindex] = allseeks[outputindex];
			left.m_materializedChildNodes[outputindex] = allMaterialized[outputindex];
			outputindex++;
			rightcount = 0;
			for (var i=0; i<rightcontent; i++) 
			{
				right.m_childKeys[i] = allkeys[outputindex];
				right.m_childBufferNumbers[i] = allseeks[outputindex];
				right.m_materializedChildNodes[i] = allMaterialized[outputindex];
				outputindex++;
				rightcount++;
			}
			right.m_childBufferNumbers[rightcount] = allseeks[outputindex];
			right.m_materializedChildNodes[rightcount] = allMaterialized[outputindex];
			left.ReParentAllChildren();
			right.ReParentAllChildren();
		}
		public static void MergeLeaves(BplusNode left, BplusNode right, out bool deleteRight) 
		{
			deleteRight = false;
			var allkeys = new string[left.m_size*2];
			var allseeks = new long[left.m_size*2];
			var index = 0;
			for (var i=0; i<left.m_size; i++) 
			{
				if (left.m_childKeys[i]==null) 
				{
					break;
				}
				allkeys[index] = left.m_childKeys[i];
				allseeks[index] = left.m_childBufferNumbers[i];
				index++;
			}
			for (var i=0; i<right.m_size; i++) 
			{
				if (right.m_childKeys[i]==null) 
				{
					break;
				}
				allkeys[index] = right.m_childKeys[i];
				allseeks[index] = right.m_childBufferNumbers[i];
				index++;
			}
			if (index<=left.m_size) 
			{
				left.Clear();
				deleteRight = true;
				for (var i=0; i<index; i++) 
				{
					left.m_childKeys[i] = allkeys[i];
					left.m_childBufferNumbers[i] = allseeks[i];
				}
				right.Free();
				left.Soil();
				return;
			}
			left.Clear();
			right.Clear();
			left.Soil();
			right.Soil();
			var rightcontent = index/2;
			var leftcontent = index - rightcontent;
			var newindex = 0;
			for (var i=0; i<leftcontent; i++) 
			{
				left.m_childKeys[i] = allkeys[newindex];
				left.m_childBufferNumbers[i] = allseeks[newindex];
				newindex++;
			}
			for (var i=0; i<rightcontent; i++) 
			{
				right.m_childKeys[i] = allkeys[newindex];
				right.m_childBufferNumbers[i] = allseeks[newindex];
				newindex++;
			}
		}
		public string DeleteLeaf(string key, out bool mergeMe) 
		{
			string result = null;
			mergeMe = false;
			var found = false;
			var deletelocation = 0;
			foreach (var thiskey in m_childKeys) 
			{
				// use comparison, not equals, in case different strings sometimes compare same
				if (thiskey!=null && m_owner.Compare(thiskey, key)==0) // thiskey.Equals(key)
				{
					found = true;
					break;
				}
				deletelocation++;
			}
			if (!found) 
			{
				throw new BplusTreeKeyMissing("cannot delete missing key: "+key);
			}
			Soil();
			// only keys are important...
			for (var i=deletelocation; i<m_size-1; i++) 
			{
				m_childKeys[i] = m_childKeys[i+1];
				m_childBufferNumbers[i] = m_childBufferNumbers[i+1];
			}
			m_childKeys[m_size-1] = null;
			//this.MaterializedChildNodes[endlocation+1] = null;
			//this.ChildBufferNumbers[endlocation+1] = BplusTreeLong.NULLBUFFERNUMBER;
			if (SizeInUse()<m_size/2)
			{
				mergeMe = true;
			}
			if (deletelocation==0) 
			{
				result = m_childKeys[0] ?? key;
				// this is only relevant for the case of 2-3 trees (empty leaf after deletion)
			}
			return result;
		}
		/// <summary>
		/// insert key/position entry in self 
		/// </summary>
		/// <param name="key">Key to associate with the leaf</param>
		/// <param name="position">position associated with key in external structur</param>
		/// <param name="splitString">if not null then the smallest key in the new split leaf</param>
		/// <param name="splitNode">if not null then the node was split and this is the leaf to the right.</param>
		/// <returns>null unless the smallest key under this node has changed, in which case it returns the smallest key.</returns>
		public string Insert(string key, long position, out string splitString, out BplusNode splitNode) 
		{
			if (IsLeaf) 
			{
				return InsertLeaf(key, position, out splitString, out splitNode);
			}
			splitString = null;
			splitNode = null;
			var insertposition = FindAtOrNextPosition(key, false);
			var insertBufferNumber = m_childBufferNumbers[insertposition];
			if (insertBufferNumber==BplusTreeLong.Nullbuffernumber) 
			{
				throw new BplusTreeException("key not followed by buffer number in non-leaf");
			}
			// insert in subtree
			var insertChild = MaterializeNodeAtIndex(insertposition);
			BplusNode childSplit;
			string childSplitString;
			var childInsert = insertChild.Insert(key, position, out childSplitString, out childSplit);
			// if there was a split the node must expand
			if (childSplit!=null) 
			{
				// insert the child
				Soil(); // redundant -- a child will have a change so this node will need to be copied
				var newChildPosition = insertposition+1;
				var dosplit = false;
				// if there is no free space we must do a split
				if (m_childBufferNumbers[m_size]!=BplusTreeLong.Nullbuffernumber) 
				{
					dosplit = true;
					PrepareForSplit();
				}
				// bubble over the current values to make space for new child
				for (var i=m_childKeys.Length-2; i>=newChildPosition-1; i--) 
				{
					var i1 = i+1;
					var i2 = i1+1;
					m_childKeys[i1] = m_childKeys[i];
					m_childBufferNumbers[i2] = m_childBufferNumbers[i1];
					var childNode = m_materializedChildNodes[i2] = m_materializedChildNodes[i1];
				}
				// record the new child
				m_childKeys[newChildPosition-1] = childSplitString;
				//this.MaterializedChildNodes[newChildPosition] = childSplit;
				//this.ChildBufferNumbers[newChildPosition] = childSplit.myBufferNumber;
				childSplit.Reparent(this, newChildPosition);
				// split, if needed
				if (dosplit) 
				{
					var splitpoint = m_materializedChildNodes.Length/2-1;
					splitString = m_childKeys[splitpoint];
					splitNode = new BplusNode(m_owner, m_parent, -1, IsLeaf);
					// make copy of expanded node structure
					var materialized = m_materializedChildNodes;
					var buffernumbers = m_childBufferNumbers;
					var keys = m_childKeys;
					// repair the expanded node
					m_childKeys = new string[m_size];
					m_materializedChildNodes = new BplusNode[m_size+1];
					m_childBufferNumbers = new long[m_size+1];
					Clear();
					Array.Copy(materialized, 0, m_materializedChildNodes, 0, splitpoint+1);
					Array.Copy(buffernumbers, 0, m_childBufferNumbers, 0, splitpoint+1);
					Array.Copy(keys, 0, m_childKeys, 0, splitpoint);
					// initialize the new node
					splitNode.Clear(); // redundant.
					var remainingKeys = m_size-splitpoint;
					Array.Copy(materialized, splitpoint+1, splitNode.m_materializedChildNodes, 0, remainingKeys+1);
					Array.Copy(buffernumbers, splitpoint+1, splitNode.m_childBufferNumbers, 0, remainingKeys+1);
					Array.Copy(keys, splitpoint+1, splitNode.m_childKeys, 0, remainingKeys);
					// fix pointers in materialized children of splitnode
					splitNode.ReParentAllChildren();
					// store the new node
					splitNode.DumpToFreshBuffer();
					splitNode.CheckIfTerminal();
					splitNode.Soil();
					CheckIfTerminal();
				}
				// fix pointers in children
				ReParentAllChildren();
			}
			if (insertposition==0) 
			{
				// the smallest key may have changed
				return childInsert;
			}
			return null;  // no change in smallest key
		}
		/// <summary>
		/// Check to see if this is a terminal node, if so record it, otherwise forget it
		/// </summary>
		void CheckIfTerminal() 
		{
			if (!IsLeaf) 
			{
				for (var i=0; i<m_size+1; i++) 
				{
					if (m_materializedChildNodes[i]!=null) 
					{
						m_owner.ForgetTerminalNode(this);
						return;
					}
				}
			}
			m_owner.RecordTerminalNode(this);
		}
		/// <summary>
		/// insert key/position entry in self (as leaf)
		/// </summary>
		/// <param name="key">Key to associate with the leaf</param>
		/// <param name="position">position associated with key in external structure</param>
		/// <param name="splitString">if not null then the smallest key in the new split leaf</param>
		/// <param name="splitNode">if not null then the node was split and this is the leaf to the right.</param>
		/// <returns>smallest key value in keys, or null if no change</returns>
		public string InsertLeaf(string key, long position, out string splitString, out BplusNode splitNode) 
		{
			splitString = null;
			splitNode = null;
			var dosplit = false;
			if (!IsLeaf) 
			{
				throw new BplusTreeException("bad call to InsertLeaf: this is not a leaf");
			}
			Soil();
			var insertposition = FindAtOrNextPosition(key, false);
			if (insertposition>=m_size) 
			{
				//throw new BplusTreeException("key too big and leaf is full");
				dosplit = true;
				PrepareForSplit();
			} 
			else 
			{
				// if it's already there then change the value at the current location (duplicate entries not supported).
				if (m_childKeys[insertposition]==null || m_owner.Compare(m_childKeys[insertposition], key)==0) // this.ChildKeys[insertposition].Equals(key)
				{
					m_childBufferNumbers[insertposition] = position;
					m_childKeys[insertposition] = key;
					if (insertposition==0) 
					{
						return key;
					} 
					else 
					{
						return null;
					}
				}
			}
			// check for a null position
			var nullindex = insertposition;
			while (nullindex<m_childKeys.Length && m_childKeys[nullindex]!=null) 
			{
				nullindex++;
			}
			if (nullindex>=m_childKeys.Length) 
			{
				if (dosplit) 
				{
					throw new BplusTreeException("can't split twice!!");
				}
				//throw new BplusTreeException("no space in leaf");
				dosplit = true;
				PrepareForSplit();
			}
			// bubble in the new info XXXX THIS SHOULD BUBBLE BACKWARDS	
			var nextkey = m_childKeys[insertposition];
			var nextposition = m_childBufferNumbers[insertposition];
			m_childKeys[insertposition] = key;
			m_childBufferNumbers[insertposition] = position;
			while (nextkey!=null) 
			{
				key = nextkey;
				position = nextposition;
				insertposition++;
				nextkey = m_childKeys[insertposition];
				nextposition = m_childBufferNumbers[insertposition];
				m_childKeys[insertposition] = key;
				m_childBufferNumbers[insertposition] = position;
			}
			// split if needed
			if (dosplit) 
			{
				var splitpoint = m_childKeys.Length/2;
				var splitlength = m_childKeys.Length - splitpoint;
				splitNode = new BplusNode(m_owner, m_parent, -1, IsLeaf);
				// copy the split info into the splitNode
				Array.Copy(m_childBufferNumbers, splitpoint, splitNode.m_childBufferNumbers, 0, splitlength);
				Array.Copy(m_childKeys, splitpoint, splitNode.m_childKeys, 0, splitlength);
				Array.Copy(m_materializedChildNodes, splitpoint, splitNode.m_materializedChildNodes, 0, splitlength);
				splitString = splitNode.m_childKeys[0];
				// archive the new node
				splitNode.DumpToFreshBuffer();
				// store the node data temporarily
				var buffernumbers = m_childBufferNumbers;
				var keys = m_childKeys;
				var nodes = m_materializedChildNodes;
				// repair current node, copy in the other part of the split
				m_childBufferNumbers = new long[m_size+1];
				m_childKeys = new string[m_size];
				m_materializedChildNodes = new BplusNode[m_size+1];
				Array.Copy(buffernumbers, 0, m_childBufferNumbers, 0, splitpoint);
				Array.Copy(keys, 0, m_childKeys, 0, splitpoint);
				Array.Copy(nodes, 0, m_materializedChildNodes, 0, splitpoint);
				for (var i=splitpoint; i<m_childKeys.Length; i++) 
				{
					m_childKeys[i] = null;
					m_childBufferNumbers[i] = BplusTreeLong.Nullbuffernumber;
					m_materializedChildNodes[i] = null;
				}
				// store the new node
				//splitNode.DumpToFreshBuffer();
				m_owner.RecordTerminalNode(splitNode);
				splitNode.Soil();
			}
			//return this.ChildKeys[0];
			if (insertposition==0) 
			{
				return key; // smallest key changed.
			} 
			else 
			{
				return null; // no change in smallest key
			}
		}
		/// <summary>
		/// Grow to this.size+1 in preparation for insertion and split
		/// </summary>
		void PrepareForSplit() 
		{
			var supersize = m_size+1;
			var positions = new long[supersize+1];
			var keys = new string[supersize];
			var materialized = new BplusNode[supersize+1];
			Array.Copy(m_childBufferNumbers, 0, positions, 0, m_size+1);
			positions[m_size+1] = BplusTreeLong.Nullbuffernumber;
			Array.Copy(m_childKeys, 0, keys, 0, m_size);
			keys[m_size] = null;
			Array.Copy(m_materializedChildNodes, 0, materialized, 0, m_size+1);
			materialized[m_size+1] = null;
			m_childBufferNumbers = positions;
			m_childKeys = keys;
			m_materializedChildNodes = materialized;
		}
		public void Load(byte[] buffer) 
		{
			// load serialized data
			// indicator | seek position | [ key storage | seek position ]*
			Clear();
			if (buffer.Length!=m_owner.Buffersize) 
			{
				throw new BplusTreeException("bad buffer size "+buffer.Length+" should be "+m_owner.Buffersize);
			}
			var indicator = buffer[0];
			IsLeaf = false;
			if (indicator==BplusTreeLong.Leaf) 
			{
				IsLeaf = true;
			} 
			else if (indicator!=BplusTreeLong.Nonleaf) 
			{
				throw new BplusTreeException("bad indicator, not leaf or nonleaf in tree "+indicator);
			}
			var index = 1;
			// get the first seek position
			m_childBufferNumbers[0] = BufferFile.RetrieveLong(buffer, index);
			var decode = Encoding.UTF8.GetDecoder();
			index+= BufferFile.Longstorage;
			var maxKeyLength = m_owner.KeyLength;
			var maxKeyPayload = maxKeyLength - BufferFile.Shortstorage;
			//this.NumberOfValidKids = 0;
			// get remaining key storages and seek positions
			var lastkey = "";
			for (var keyIndex=0; keyIndex<m_size; keyIndex++) 
			{
				// decode and store a key
				var keylength = BufferFile.RetrieveShort(buffer, index);
				if (keylength<-1 || keylength>maxKeyPayload) 
				{
					throw new BplusTreeException("invalid keylength decoded");
				}
				index+= BufferFile.Shortstorage;
				string key = null;
				if (keylength==0) 
				{
					key = "";
				} 
				else if (keylength>0) 
				{
					var charCount = decode.GetCharCount(buffer, index, keylength);
					var ca = new char[charCount];
					decode.GetChars(buffer, index, keylength, ca, 0);
					//this.NumberOfValidKids++;
					key = new String(ca);
				}
				m_childKeys[keyIndex] = key;
				index+= maxKeyPayload;
				// decode and store a seek position
				var seekPosition = BufferFile.RetrieveLong(buffer, index);
				if (!IsLeaf) 
				{
					if (key==null & seekPosition!=BplusTreeLong.Nullbuffernumber) 
					{
						throw new BplusTreeException("key is null but position is not "+keyIndex);
					} 
					else if (lastkey==null && key!=null) 
					{
						throw new BplusTreeException("null key followed by non-null key "+keyIndex);
					}
				}
				lastkey = key;
				m_childBufferNumbers[keyIndex+1] = seekPosition;
				index+= BufferFile.Longstorage;
			}
		}
		/// <summary>
		/// check that key is ok for node of this size (put here for locality of relevant code).
		/// </summary>
		/// <param name="key">key to check</param>
		/// <param name="owner">tree to contain node containing the key</param>
		/// <returns>true if key is ok</returns>
		public static bool KeyOK(string key, BplusTreeLong owner) 
		{
			if (key==null) 
			{ 
				return false;
			}
			var encode = Encoding.UTF8.GetEncoder();
			var maxKeyLength = owner.KeyLength;
			var maxKeyPayload = maxKeyLength - BufferFile.Shortstorage;
			var keyChars = key.ToCharArray();
			var charCount = encode.GetByteCount(keyChars, 0, keyChars.Length, true);
			if (charCount>maxKeyPayload) 
			{
				return false;
			}
			return true;
		}
		public void Dump(byte[] buffer) 
		{
			// indicator | seek position | [ key storage | seek position ]*
			if (buffer.Length!=m_owner.Buffersize) 
			{
				throw new BplusTreeException("bad buffer size "+buffer.Length+" should be "+m_owner.Buffersize);
			}
			buffer[0] = BplusTreeLong.Nonleaf;
			if (IsLeaf) { buffer[0] = BplusTreeLong.Leaf; }
			var index = 1;
			// store first seek position
			BufferFile.Store(m_childBufferNumbers[0], buffer, index);
			index+= BufferFile.Longstorage;
			var encode = Encoding.UTF8.GetEncoder();
			// store remaining keys and seeks
			var maxKeyLength = m_owner.KeyLength;
			var maxKeyPayload = maxKeyLength - BufferFile.Shortstorage;
			var lastkey = "";
			for (var keyIndex=0; keyIndex<m_size; keyIndex++) 
			{
				// store a key
				var theKey = m_childKeys[keyIndex];
				short charCount = -1;
				if (theKey!=null) 
				{
					var keyChars = theKey.ToCharArray();
					charCount = (short) encode.GetByteCount(keyChars, 0, keyChars.Length, true);
					if (charCount>maxKeyPayload) 
					{
						throw new BplusTreeException("string bytes to large for use as key "+charCount+">"+maxKeyPayload);
					}
					BufferFile.Store(charCount, buffer, index);
					index+= BufferFile.Shortstorage;
					encode.GetBytes(keyChars, 0, keyChars.Length, buffer, index, true);
				} 
				else 
				{
					// null case (no string to read)
					BufferFile.Store(charCount, buffer, index);
					index+= BufferFile.Shortstorage;
				}
				index+= maxKeyPayload;
				// store a seek
				var seekPosition = m_childBufferNumbers[keyIndex+1];
				if (theKey==null && seekPosition!=BplusTreeLong.Nullbuffernumber && !IsLeaf) 
				{
					throw new BplusTreeException("null key paired with non-null location "+keyIndex);
				}
				if (lastkey==null && theKey!=null) 
				{
					throw new BplusTreeException("null key followed by non-null key "+keyIndex);
				}
				lastkey = theKey;
				BufferFile.Store(seekPosition, buffer, index);
				index+= BufferFile.Longstorage;
			}
		}
		/// <summary>
		/// Close the node:
		/// invalidate all children, store state if needed, remove materialized self from parent.
		/// </summary>
		public long Invalidate(bool destroyRoot) 
		{
			var result = MyBufferNumber;
			if (!IsLeaf) 
			{
				// need to invalidate kids
				for (var i=0; i<m_size+1; i++) 
				{
					if (m_materializedChildNodes[i]!=null) 
					{
						// new buffer numbers are recorded automatically
						m_childBufferNumbers[i] = m_materializedChildNodes[i].Invalidate(true);
					}
				}
			} 
			// store if dirty
			if (m_dirty) 
			{
				result = DumpToFreshBuffer();
//				result = this.myBufferNumber;
			}
			// remove from owner archives if present
			m_owner.ForgetTerminalNode(this);
			// remove from parent
			if (m_parent!=null && m_indexInParent>=0) 
			{
				m_parent.m_materializedChildNodes[m_indexInParent] = null;
				m_parent.m_childBufferNumbers[m_indexInParent] = result; // should be redundant
				m_parent.CheckIfTerminal();
				m_indexInParent = -1;
			}
			// render all structures useless, just in case...
			if (destroyRoot) 
			{
				Destroy();
			}
			return result;
		}
		/// <summary>
		/// Mark this as dirty and all ancestors too.
		/// </summary>
		void Soil() 
		{
			if (m_dirty) 
			{
			} 
			else 
			{
				m_dirty = true;
				if (m_parent!=null) 
				{
					m_parent.Soil();
				}
			}
		}
		public void AsHtml(StringBuilder sb) 
		{
			var hygeine = "clean";
			if (m_dirty) { hygeine = "dirty"; }
			var keycount = 0;
			if (IsLeaf) 
			{
				for (var i=0; i<m_size; i++) 
				{
					var key = m_childKeys[i];
					var seek = m_childBufferNumbers[i];
					if (key!=null) 
					{
						key = PrintableString(key);
						sb.Append("'"+key+"' : "+seek+"<br>\r\n");
						keycount++;
					}
				}
				sb.Append("leaf "+m_indexInParent+" at "+MyBufferNumber+" #keys=="+keycount+" "+hygeine+"\r\n");
			} 
			else 
			{
				sb.Append("<table border>\r\n");
				sb.Append("<tr><td colspan=2>nonleaf "+m_indexInParent+" at "+MyBufferNumber+" "+hygeine+"</td></tr>\r\n");
				if (m_childBufferNumbers[0]!=BplusTreeLong.Nullbuffernumber) 
				{
					MaterializeNodeAtIndex(0);
					sb.Append("<tr><td></td><td>"+m_childBufferNumbers[0]+"</td><td>\r\n");
					m_materializedChildNodes[0].AsHtml(sb);
					sb.Append("</td></tr>\r\n");
				}
				for (var i=0; i<m_size; i++) 
				{
					var key =  m_childKeys[i];
					if (key==null) 
					{
						break;
					}
					key = PrintableString(key);
					sb.Append("<tr><th>'"+key+"'</th><td></td><td></td></tr>\r\n");
					try 
					{
						MaterializeNodeAtIndex(i+1);
						sb.Append("<tr><td></td><td>"+m_childBufferNumbers[i+1]+"</td><td>\r\n");
						m_materializedChildNodes[i+1].AsHtml(sb);
						sb.Append("</td></tr>\r\n");
					} 
					catch (BplusTreeException) 
					{
						sb.Append("<tr><td></td><th>COULDN'T MATERIALIZE NODE "+(i+1)+"</th></tr>");
					}
					keycount++;
				}
				sb.Append("<tr><td colspan=2> #keys=="+keycount+"</td></tr>\r\n");
				sb.Append("</table>\r\n");
			}
		}
		public static string PrintableString(string s) 
		{
			if (s==null) { return "[NULL]"; }
			var sb = new StringBuilder();
			foreach (var c in s) 
			{
				if (Char.IsLetterOrDigit(c) || Char.IsPunctuation(c)) 
				{
					sb.Append(c);
				} 
				else 
				{
					sb.Append("["+Convert.ToInt32(c)+"]");
				}
			}
			return sb.ToString();
		}
	}
	/// <summary>
	/// Generic error including programming errors.
	/// </summary>
	public class BplusTreeException: ApplicationException 
	{
		public BplusTreeException(string message): base(message) 
		{
			// do nothing extra
		}
	}
	/// <summary>
	/// No such key found for attempted retrieval.
	/// </summary>
	public class BplusTreeKeyMissing: ApplicationException 
	{
		public BplusTreeKeyMissing(string message): base(message) 
		{
			// do nothing extra
		}
	}
	/// <summary>
	/// Key cannot be null or too large.
	/// </summary>
	public class BplusTreeBadKeyValue: ApplicationException 
	{
		public BplusTreeBadKeyValue(string message): base(message) 
		{
			// do nothing extra
		}
	}
}
