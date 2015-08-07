using System;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace BplusDotNet
{
	/// <summary>
	/// Btree mapping unlimited length key strings to fixed length hash values
	/// </summary>
	public class hBplusTreeBytes: xBplusTreeBytes
	{
		public hBplusTreeBytes(BplusTreeBytes tree, int hashLength) : base(tree, hashLength)
		{
			// null out the culture context to use the naive comparison
			this.tree.NoCulture();
		}
		
		public new static hBplusTreeBytes Initialize(string treefileName, string blockfileName, int PrefixLength, int cultureId,
			int nodesize, int buffersize) 
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, cultureId, nodesize, buffersize),
				PrefixLength);
		}
		public new static hBplusTreeBytes Initialize(string treefileName, string blockfileName, int PrefixLength, int cultureId) 
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength, cultureId),
				PrefixLength);
		}
		public new static hBplusTreeBytes Initialize(string treefileName, string blockfileName, int PrefixLength) 
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefileName, blockfileName, PrefixLength),
				PrefixLength);
		}
		public new static hBplusTreeBytes Initialize(Stream treefile, Stream blockfile, int PrefixLength, int cultureId,
			int nodesize, int buffersize) 
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, cultureId, nodesize, buffersize),
				PrefixLength);
		}
		public new static hBplusTreeBytes Initialize(Stream treefile, Stream blockfile, int PrefixLength, int cultureId) 
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefile, blockfile, PrefixLength, cultureId),
				PrefixLength);
		}
		public new static hBplusTreeBytes Initialize(Stream treefile, Stream blockfile, int PrefixLength) 
		{
			return new hBplusTreeBytes(
				BplusTreeBytes.Initialize(treefile, blockfile, PrefixLength),
				PrefixLength);
		}

		public new static hBplusTreeBytes ReOpen(Stream treefile, Stream blockfile) 
		{
			BplusTreeBytes tree = BplusTreeBytes.ReOpen(treefile, blockfile);
			int prefixLength = tree.MaxKeyLength();
			return new hBplusTreeBytes(tree, prefixLength);
		}
		public new static hBplusTreeBytes ReOpen(string treefileName, string blockfileName) 
		{
			BplusTreeBytes tree = BplusTreeBytes.ReOpen(treefileName, blockfileName);
			int prefixLength = tree.MaxKeyLength();
			return new hBplusTreeBytes(tree, prefixLength);
		}
		public new static hBplusTreeBytes ReadOnly(string treefileName, string blockfileName) 
		{
			BplusTreeBytes tree = BplusTreeBytes.ReadOnly(treefileName, blockfileName);
			int prefixLength = tree.MaxKeyLength();
			return new hBplusTreeBytes(tree, prefixLength);
		}

		public override string PrefixForByteCount(string s, int maxbytecount)
		{
			byte[] inputbytes = BplusTree.StringToBytes(s);
			MD5 d = MD5.Create();
			byte[] digest = d.ComputeHash(inputbytes);
			var resultbytes = new byte[maxbytecount];
			// copy digest translating to printable ascii
			for (int i=0; i<maxbytecount; i++) 
			{
				int r = digest[i % digest.Length];
				if (r>127) 
				{
					r = 256-r;
				}
				if (r<0) 
				{
					r = -r;
				}
				//Console.WriteLine(" before "+i+" "+r);
				r = r%79 + 40; // printable ascii
				//Console.WriteLine(" "+i+" "+r);
				resultbytes[i] = (byte)r;
			}
			string result = BplusTree.BytesToString(resultbytes);
			return result;
		}
		public string toHtml() 
		{
			var sb = new StringBuilder();
			sb.Append(tree.toHtml());
			sb.Append("\r\n<br><b>key / hash / value dump</b><br>");
			string currentkey = FirstKey();
			while (currentkey!=null) 
			{
				sb.Append("\r\n<br>"+currentkey);
				sb.Append(" / "+BplusNode.PrintableString(PrefixForByteCount(currentkey, prefixLength)));
				try 
				{
					sb.Append( " / value found " );
				}
				catch (Exception) 
				{
					sb.Append( " !!!!!!! FAILED TO GET VALUE");
				}
				currentkey = NextKey(currentkey);
			}
			return sb.ToString();
		}
	}
}
