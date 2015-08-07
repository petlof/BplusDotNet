using System.IO;

namespace BplusDotNet
{
	/// <summary>
	/// Tree index mapping strings to strings with unlimited key length
	/// </summary>
	public class hBplusTree : BplusTree
	{
	    readonly hBplusTreeBytes m_xtree;
		public hBplusTree(hBplusTreeBytes tree) : base(tree)
		{
			m_xtree = tree;
		}
		protected override bool checkTree()
		{
			return false;
		}
		public void LimitBucketSize(int limit) 
		{
			m_xtree.BucketSizeLimit = limit;
		}
		public static new hBplusTree Initialize(string treefileName, string blockfileName, int prefixLength, int cultureId,
			int nodesize, int buffersize) 
		{
			hBplusTreeBytes tree = hBplusTreeBytes.Initialize(treefileName, blockfileName, prefixLength, cultureId, nodesize, buffersize);
			return new hBplusTree(tree);
		}
		public static new hBplusTree Initialize(string treefileName, string blockfileName, int prefixLength, int cultureId) 
		{
			hBplusTreeBytes tree = hBplusTreeBytes.Initialize(treefileName, blockfileName, prefixLength, cultureId);
			return new hBplusTree(tree);
		}
		public static new hBplusTree Initialize(string treefileName, string blockfileName, int prefixLength) 
		{
			hBplusTreeBytes tree = hBplusTreeBytes.Initialize(treefileName, blockfileName, prefixLength);
			return new hBplusTree(tree);
		}
		
		public static new hBplusTree Initialize(Stream treefile, Stream blockfile, int prefixLength, int cultureId,
			int nodesize, int buffersize) 
		{
			hBplusTreeBytes tree = hBplusTreeBytes.Initialize(treefile, blockfile, prefixLength, cultureId, nodesize, buffersize);
			return new hBplusTree(tree);
		}
		public static new hBplusTree Initialize(Stream treefile, Stream blockfile, int prefixLength, int cultureId) 
		{
			hBplusTreeBytes tree = hBplusTreeBytes.Initialize(treefile, blockfile, prefixLength, cultureId);
			return new hBplusTree(tree);
		}
		public static new hBplusTree Initialize(Stream treefile, Stream blockfile, int keyLength) 
		{
			hBplusTreeBytes tree = hBplusTreeBytes.Initialize(treefile, blockfile, keyLength);
			return new hBplusTree(tree);
		}
		
		public static new hBplusTree ReOpen(Stream treefile, Stream blockfile) 
		{
			hBplusTreeBytes tree = hBplusTreeBytes.ReOpen(treefile, blockfile);
			return new hBplusTree(tree);
		}
		public static new hBplusTree ReOpen(string treefileName, string blockfileName) 
		{
			hBplusTreeBytes tree = hBplusTreeBytes.ReOpen(treefileName, blockfileName);
			return new hBplusTree(tree);
		}
		public static new hBplusTree ReadOnly(string treefileName, string blockfileName) 
		{
			hBplusTreeBytes tree = hBplusTreeBytes.ReadOnly(treefileName, blockfileName);
			return new hBplusTree(tree);
		}
		public override string toHtml() 
		{
			return ((hBplusTreeBytes) Tree).toHtml();
		}
	}
}
