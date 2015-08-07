using System;

namespace encodingTest
{
	/// <summary>
	/// Summary description for Class1.
	/// </summary>
	class EncodingTest
	{
		/// <summary>
		/// The main entry point for the application.
		/// </summary>
		[STAThread]
		public static void Main() 
		{
			if (System.IO.File.Exists("/tmp/junk.bin")) 
			{
				System.IO.File.Delete("/tmp/junk.bin");
				System.IO.File.Delete("/tmp/junk2.bin");
			}
			BplusDotNet.hBplusTreeBytes HT = (BplusDotNet.hBplusTreeBytes) 
				BplusDotNet.hBplusTreeBytes.Initialize("/tmp/junk.bin", "/tmp/junk2.bin", 6);
			String stuff = "cæser";
			String test = HT.PrefixForByteCount(stuff, 5);
			Console.WriteLine("test="+test);
			HT[stuff] = BplusDotNet.BplusTree.StringToBytes( "goober");
		}

	}
}
