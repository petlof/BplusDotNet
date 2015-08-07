using System.Collections;
using System.Globalization;
using System.IO;
using System.Text;

namespace BplusDotNet
{
	/// <summary>
	/// BPlus tree implementation mapping strings to bytes with fixed key length
	/// </summary>
	public class BplusTreeBytes : IByteTree
	{
	    readonly BplusTreeLong m_tree;
	    readonly LinkedFile m_archive;
	    readonly Hashtable m_freeChunksOnCommit = new Hashtable();
	    readonly Hashtable m_freeChunksOnAbort = new Hashtable();
	    private const int Defaultblocksize = 1024;
	    private const int Defaultnodesize = 32;

	    public BplusTreeBytes(BplusTreeLong tree, LinkedFile archive)
		{
			m_tree = tree;
			m_archive = archive;
		}

		public static BplusTreeBytes Initialize(string treefileName, string blockfileName, int keyLength, int cultureId,
			int nodesize, int buffersize) 
		{
			Stream treefile = new FileStream(treefileName, FileMode.CreateNew, 
				FileAccess.ReadWrite);
			Stream blockfile = new FileStream(blockfileName, FileMode.CreateNew, 
				FileAccess.ReadWrite);
			return Initialize(treefile, blockfile, keyLength, cultureId, nodesize, buffersize);
		}
		public static BplusTreeBytes Initialize(string treefileName, string blockfileName, int keyLength, int cultureId) 
		{
			Stream treefile = new FileStream(treefileName, FileMode.CreateNew, 
				FileAccess.ReadWrite);
			Stream blockfile = new FileStream(blockfileName, FileMode.CreateNew, 
				FileAccess.ReadWrite);
			return Initialize(treefile, blockfile, keyLength, cultureId);
		}
		public static BplusTreeBytes Initialize(string treefileName, string blockfileName, int keyLength) 
		{
			Stream treefile = new FileStream(treefileName, FileMode.CreateNew, 
				FileAccess.ReadWrite);
			Stream blockfile = new FileStream(blockfileName, FileMode.CreateNew, 
				FileAccess.ReadWrite);
			return Initialize(treefile, blockfile, keyLength);
		}
		public static BplusTreeBytes Initialize(Stream treefile, Stream blockfile, int keyLength, int cultureId,
			int nodesize, int buffersize) 
		{
			BplusTreeLong tree = BplusTreeLong.InitializeInStream(treefile, keyLength, nodesize, cultureId);
			LinkedFile archive = LinkedFile.InitializeLinkedFileInStream(blockfile, buffersize);
			return new BplusTreeBytes(tree, archive);
		}
		public static BplusTreeBytes Initialize(Stream treefile, Stream blockfile, int keyLength, int cultureId) 
		{
			return Initialize(treefile, blockfile, keyLength, cultureId, Defaultnodesize, Defaultblocksize);
		}
		public static BplusTreeBytes Initialize(Stream treefile, Stream blockfile, int keyLength) 
		{
			int cultureId = CultureInfo.InvariantCulture.LCID;
			return Initialize(treefile, blockfile, keyLength, cultureId, Defaultnodesize, Defaultblocksize);
		}
		public static BplusTreeBytes ReOpen(Stream treefile, Stream blockfile) 
		{
			BplusTreeLong tree = BplusTreeLong.SetupFromExistingStream(treefile);
			LinkedFile archive = LinkedFile.SetupFromExistingStream(blockfile);
			return new BplusTreeBytes(tree, archive);
		}
		public static BplusTreeBytes ReOpen(string treefileName, string blockfileName, FileAccess access) 
		{
			Stream treefile = new FileStream(treefileName, FileMode.Open, 
				access);
			Stream blockfile = new FileStream(blockfileName, FileMode.Open, 
				access);
			return ReOpen(treefile, blockfile);
		}
		public static BplusTreeBytes ReOpen(string treefileName, string blockfileName) 
		{
			return ReOpen(treefileName, blockfileName, FileAccess.ReadWrite);
		}
		public static BplusTreeBytes ReadOnly(string treefileName, string blockfileName)
		{
			return ReOpen(treefileName, blockfileName, FileAccess.Read);
		}

		/// <summary>
		/// Use non-culture sensitive total order on binary strings.
		/// </summary>
		public void NoCulture() 
		{
			m_tree.DontUseCulture = true;
			m_tree.CultureContext = null;
		}
		public int MaxKeyLength() 
		{
			return m_tree.MaxKeyLength();
		}
		#region ITreeIndex Members

		
		public int Compare(string left, string right) 
		{
			return m_tree.Compare(left, right);
		}
		public void Shutdown()
		{
			m_tree.Shutdown();
			m_archive.Shutdown();
		}

		public void Recover(bool correctErrors)
		{
			m_tree.Recover(correctErrors);
			var chunksInUse = new Hashtable();
			string key = m_tree.FirstKey();
			while (key!=null) 
			{
				long buffernumber = m_tree[key];
				if (chunksInUse.ContainsKey(buffernumber)) 
				{
					throw new BplusTreeException("buffer number "+buffernumber+" associated with more than one key '"
						+key+"' and '"+chunksInUse[buffernumber]+"'");
				}
				chunksInUse[buffernumber] = key;
				key = m_tree.NextKey(key);
			}
			// also consider the un-deallocated chunks to be in use
			foreach (DictionaryEntry thing in m_freeChunksOnCommit) 
			{
				var buffernumber = (long) thing.Key;
				chunksInUse[buffernumber] = "awaiting commit";
			}
			m_archive.Recover(chunksInUse, correctErrors);
		}

		public void RemoveKey(string key)
		{
			long map = m_tree[key];
			//this.archive.ReleaseBuffers(map);
			//this.FreeChunksOnCommit.Add(map);
			if (m_freeChunksOnAbort.ContainsKey(map)) 
			{
				// free it now
				m_freeChunksOnAbort.Remove(map);
				m_archive.ReleaseBuffers(map);
			} 
			else 
			{
				// free when committed
				m_freeChunksOnCommit[map] = map;
			}
			m_tree.RemoveKey(key);
		}

		public string FirstKey()
		{
			return m_tree.FirstKey();
		}

		public string NextKey(string afterThisKey)
		{
			return m_tree.NextKey(afterThisKey);
		}

		public bool ContainsKey(string key)
		{
			return m_tree.ContainsKey(key);
		}

		public object Get(string key, object defaultValue)
		{
			long map;
			if (m_tree.ContainsKey(key, out map)) 
			{
				return m_archive.GetChunk(map);
			}
			return defaultValue;
		}

		public void Set(string key, object map)
		{
			if (!(map is byte[]) )
			{
				throw new BplusTreeBadKeyValue("BplusTreeBytes can only archive byte array as value");
			}
			var thebytes = (byte[]) map;
			this[key] = thebytes;
		}
		public byte[] this[string key] 
		{
			set 
			{
				long storage = m_archive.StoreNewChunk(value, 0, value.Length);
				//this.FreeChunksOnAbort.Add(storage);
				m_freeChunksOnAbort[storage] = storage;
				long valueFound;
				if (m_tree.ContainsKey(key, out valueFound)) 
				{
					//this.archive.ReleaseBuffers(valueFound);
					if (m_freeChunksOnAbort.ContainsKey(valueFound)) 
					{
						// free it now
						m_freeChunksOnAbort.Remove(valueFound);
						m_archive.ReleaseBuffers(valueFound);
					} 
					else 
					{
						// release at commit.
						m_freeChunksOnCommit[valueFound] = valueFound;
					}
				}
				m_tree[key] = storage;
			}
			get 
			{
				long map = m_tree[key];
				return m_archive.GetChunk(map);
			}
		}

		public void Commit()
		{
			// store all new bufferrs
			m_archive.Flush();
			// commit the tree
			m_tree.Commit();
			// at this point the new buffers have been committed, now free the old ones
			//this.FreeChunksOnCommit.Sort();
			var toFree = new ArrayList();
			foreach (DictionaryEntry d in m_freeChunksOnCommit) 
			{
				toFree.Add(d.Key);
			}
			toFree.Sort();
			toFree.Reverse();
			foreach (object thing in toFree) 
			{
				var chunknumber = (long) thing;
				m_archive.ReleaseBuffers(chunknumber);
			}
			m_archive.Flush();
			ClearBookKeeping();
		}

		public void Abort()
		{
			//this.FreeChunksOnAbort.Sort();
			var toFree = new ArrayList();
			foreach (DictionaryEntry d in m_freeChunksOnAbort) 
			{
				toFree.Add(d.Key);
			}
			toFree.Sort();
			toFree.Reverse();
			foreach (object thing in toFree) 
			{
				var chunknumber = (long) thing;
				m_archive.ReleaseBuffers(chunknumber);
			}
			m_tree.Abort();
			m_archive.Flush();
			ClearBookKeeping();
		}
		
		public void SetFootPrintLimit(int limit) 
		{
			m_tree.SetFootPrintLimit(limit);
		}

		void ClearBookKeeping() 
		{
			m_freeChunksOnCommit.Clear();
			m_freeChunksOnAbort.Clear();
		}

		#endregion

		public string toHtml() 
		{
			string treehtml = m_tree.toHtml();
			var sb = new StringBuilder();
			sb.Append(treehtml);
			sb.Append("\r\n<br> free on commit "+m_freeChunksOnCommit.Count+" ::");
			foreach (DictionaryEntry thing in m_freeChunksOnCommit) 
			{
				sb.Append(" "+thing.Key);
			}
			sb.Append("\r\n<br> free on abort "+m_freeChunksOnAbort.Count+" ::");
			foreach (DictionaryEntry thing in m_freeChunksOnAbort) 
			{
				sb.Append(" "+thing.Key);
			}
			return sb.ToString(); // archive info not included
		}
	}
}
