using System;
using System.IO;
using System.Text;

namespace BplusDotNet
{
	/// <summary>
	/// Tree index mapping strings to strings.
	/// </summary>
	public class BplusTree : IStringTree
	{
		/// <summary>
		/// Internal tree mapping strings to bytes (for conversion to strings).
		/// </summary>
		public ITreeIndex Tree;
		public BplusTree(ITreeIndex tree)
		{
			if (!(tree is BplusTreeBytes) && checkTree()) 
			{
				throw new BplusTreeException("Bplustree (superclass) can only wrap BplusTreeBytes, not other ITreeIndex implementations");
			}
			Tree = tree;
		}

	    protected virtual bool checkTree() 
		{
			// this is to prevent accidental misuse with the wrong ITreeIndex implementation,
			// but to also allow subclasses to override the behaviour... (there must be a better way...)
			return true;
		}

		public static BplusTree Initialize(string treefileName, string blockfileName, int keyLength, int cultureId,
			int nodesize, int buffersize) 
		{
			BplusTreeBytes tree = BplusTreeBytes.Initialize(treefileName, blockfileName, keyLength, cultureId, nodesize, buffersize);
			return new BplusTree(tree);
		}
		public static BplusTree Initialize(string treefileName, string blockfileName, int keyLength, int cultureId) 
		{
			BplusTreeBytes tree = BplusTreeBytes.Initialize(treefileName, blockfileName, keyLength, cultureId);
			return new BplusTree(tree);
		}
		public static BplusTree Initialize(string treefileName, string blockfileName, int keyLength) 
		{
			BplusTreeBytes tree = BplusTreeBytes.Initialize(treefileName, blockfileName, keyLength);
			return new BplusTree(tree);
		}
		
		public static BplusTree Initialize(Stream treefile, Stream blockfile, int keyLength, int cultureId,
			int nodesize, int buffersize) 
		{
			BplusTreeBytes tree = BplusTreeBytes.Initialize(treefile, blockfile, keyLength, cultureId, nodesize, buffersize);
			return new BplusTree(tree);
		}
		public static BplusTree Initialize(Stream treefile, Stream blockfile, int keyLength, int cultureId) 
		{
			BplusTreeBytes tree = BplusTreeBytes.Initialize(treefile, blockfile, keyLength, cultureId);
			return new BplusTree(tree);
		}
		public static BplusTree Initialize(Stream treefile, Stream blockfile, int keyLength) 
		{
			BplusTreeBytes tree = BplusTreeBytes.Initialize(treefile, blockfile, keyLength);
			return new BplusTree(tree);
		}
		
		public static BplusTree ReOpen(Stream treefile, Stream blockfile) 
		{
			BplusTreeBytes tree = BplusTreeBytes.ReOpen(treefile, blockfile);
			return new BplusTree(tree);
		}
		public static BplusTree ReOpen(string treefileName, string blockfileName) 
		{
			BplusTreeBytes tree = BplusTreeBytes.ReOpen(treefileName, blockfileName);
			return new BplusTree(tree);
		}
		public static BplusTree ReadOnly(string treefileName, string blockfileName) 
		{
			BplusTreeBytes tree = BplusTreeBytes.ReadOnly(treefileName, blockfileName);
			return new BplusTree(tree);
		}

		#region ITreeIndex Members

		public void Recover(bool correctErrors)
		{
			Tree.Recover(correctErrors);
		}

		public void RemoveKey(string key)
		{
			Tree.RemoveKey(key);
		}

		public string FirstKey()
		{
			return Tree.FirstKey();
		}

		public string NextKey(string afterThisKey)
		{
			return Tree.NextKey(afterThisKey);
		}

		public bool ContainsKey(string key)
		{
			return Tree.ContainsKey(key);
		}

		public object Get(string key, object defaultValue)
		{
			object test = Tree.Get(key, "");
			if (test is byte[]) 
			{
				return BytesToString((byte[]) test);
			}
			return defaultValue;
		}

		public void Set(string key, object map)
		{
			if (!(map is string)) 
			{
				throw new BplusTreeException("BplusTree only stores strings as values");
			}
			var thestring = (string) map;
			byte[] bytes = StringToBytes(thestring);
			//this.tree[key] = bytes;
			Tree.Set(key, bytes);
		}

		public void Commit()
		{
			Tree.Commit();
		}

		public void Abort()
		{
			Tree.Abort();
		}

		
		public void SetFootPrintLimit(int limit) 
		{
			Tree.SetFootPrintLimit(limit);
		}
		
		public void Shutdown()
		{
			Tree.Shutdown();
		}
		
		public int Compare(string left, string right) 
		{
			return Tree.Compare(left, right);
		}

		#endregion
		public string this[string key] 
		{
			get 
			{
				object theGet = Tree.Get(key, "");
				if (theGet is byte[]) 
				{
					var bytes = (byte[]) theGet;
					return BytesToString(bytes);
				}
				//System.Diagnostics.Debug.WriteLine(this.toHtml());
				throw new BplusTreeKeyMissing("key not found "+key);
			} 
			set 
			{
				byte[] bytes = StringToBytes(value);
				//this.tree[key] = bytes;
				Tree.Set(key, bytes);
			}
		}
		public static string BytesToString(byte[] bytes) 
		{
			Decoder decode = Encoding.UTF8.GetDecoder();
			long length = decode.GetCharCount(bytes, 0, bytes.Length);
			var chars = new char[length];
			decode.GetChars(bytes, 0, bytes.Length, chars, 0);
			var result = new String(chars);
			return result;
		}
		public static byte[] StringToBytes(string thestring) 
		{
			Encoder encode = Encoding.UTF8.GetEncoder();
			char[] chars = thestring.ToCharArray();
			long length = encode.GetByteCount(chars, 0, chars.Length, true);
			var bytes = new byte[length];
			encode.GetBytes(chars, 0, chars.Length,bytes, 0, true);
			return bytes;
		}
		public virtual string toHtml() 
		{
			return ((BplusTreeBytes) Tree).toHtml();
		}
	}
}
