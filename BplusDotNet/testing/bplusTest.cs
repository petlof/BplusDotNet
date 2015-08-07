using System;
using System.Diagnostics;
using System.Collections;

namespace testing
{

	/// <summary>
	/// Serializable class for testing serializable tree.
	/// </summary>
	[Serializable]
	public class SerializableThing
	{
		public int i, j, k;
		public SerializableThing(int i, int j, int k) 
		{
			this.i = i;
			this.j = j;
			this.k = k;
		}
		public override bool Equals(object obj)
		{
			if (! (obj is SerializableThing)) 
			{
				return false;
			}
			SerializableThing other = (SerializableThing) obj;
			return (other.i == this.i) && (other.j == this.j) && (other.k == this.k);
		}
		public override int GetHashCode()
		{
			return this.i^(this.j<<8)^(this.k<<16);
		}

	}

	/// <summary>
	/// tests main entry point for bplusdotnet.  Throws exception on failure.
	/// </summary>
	public class bplusTest
	{
		static string tempdirectory = @"c:\tmp"; // set to a directory to test storing to/from files
		static Hashtable allinserts = new Hashtable();
		static Hashtable lastcommittedinserts = new Hashtable();
		static bool full = true;
		static int keylength = 20;
		static int prefixlength = 6;
		static int nodesize = 6;
		static int buffersize = 100;
		static int bucketsizelimit = 100; // sanity check
		static bool DoAllTests = true;
		public static void Main() 
		{
			if (DoAllTests) 
			{
				byteStringTest();
				intTests();
				longTests();
				shortTests();
				//BplusDotNet.BufferFile nullbf = new BplusDotNet.BufferFile(null, 90);
				testBufferFile();
				LinkedFileTest();
				BplusTreeLongTest();
				//bplustreetest();
				Test();
				xTest();
				hTest();
				sTest();
			}
			CompatTest();
		}
		static string keyMaker(int i, int j, int k) 
		{
			int selector = (i+j+k)%3;
			string result = ""+i+"."+j+"."+k;
			if (selector==0) 
			{
				result = ""+k+"."+(j%5)+"."+i;
			} 
			else if (selector==1) 
			{
				result = ""+k+"."+j+"."+i;
			}
			return result;
		}
		static string xkeyMaker(int i, int j, int k) 
		{
			string result = keyMaker(i,j,k);
			result = result+result+result;
			result = result + keyMaker(k,i,j);
			return result;
		}
		static string ValueMaker(int i, int j, int k)
		{
			if ( ((i+j+k)%5) == 3 )
			{
				return "";
			}
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			sb.Append("value");
			for (int x=0; x<i+k*5; x++) 
			{
				sb.Append(j);
				sb.Append(k);
			}
			return sb.ToString();
		}

		public static void hTest() 
		{
			string watchKey = ""; //"4.3.04.3.04.3.03.0.4";
			Console.WriteLine("TESTING HBPLUSTREE");
			System.IO.Stream treefile=null, blockfile=null;
			BplusDotNet.hBplusTree bpt = getHTree(null, ref treefile, ref blockfile);
			Hashtable allmaps = new Hashtable();
			for (int i=0; i<10; i++) 
			{
				Console.WriteLine("Pass "+i+" of 10");
				bpt.SetFootPrintLimit(16-i);
				for (int j=0; j<30; j++) 
				{
					Hashtable record = new Hashtable();
					for (int k=0; k<30; k++) 
					{
						string thiskey = xkeyMaker(i,j,k);
						string thisvalue = ValueMaker(j,k,i);
						if (thiskey.Equals(watchKey)) 
						{
							Debug.WriteLine("<br> NOW SETTING "+watchKey);
						}
						record[thiskey] = thisvalue;
						bpt[thiskey] = thisvalue;
						if (thiskey.Equals(watchKey)) 
						{
							Debug.WriteLine("<br> AFTER SETTING "+bpt.toHtml());
						}
					}
					if ((j%3)==1) 
					{
						bpt.Recover(false);
					}
					if ( ((i+j)%2) == 1 ) 
					{
						bpt.Commit();
						bpt.Abort();  // should have no effect
						bpt.Commit();  // ditto
						if ( (i+j)%5 < 2) 
						{
							//Debug.WriteLine(bpt.toHtml());
							foreach (DictionaryEntry d in record) 
							{
								string thiskey = (string) d.Key;
								bpt.RemoveKey(thiskey);
								if (thiskey.Equals(watchKey)) 
								{
									Debug.WriteLine("<br> NOW REMOVING "+watchKey);
								}
								if (allmaps.ContainsKey(thiskey)) 
								{
									allmaps.Remove(thiskey);
								}
							}
							//Debug.WriteLine(bpt.toHtml());
							bpt.Commit();
							//return;
						} 
						else 
						{
							foreach (DictionaryEntry d in record) 
							{
								allmaps[d.Key] = d.Value;
							}
						}
					} 
					else 
					{
						bpt.Abort();
					}
					if ((j%4)==2) 
					{
						bpt = getHTree(bpt, ref treefile, ref blockfile);
					}
					// now check the structure
					//ArrayList allkeys = new ArrayList();
					foreach (DictionaryEntry d in allmaps) 
					{
						string thiskey = (string)d.Key;
						string thisvalue = (string)d.Value;
						if (thiskey.Equals(watchKey)) 
						{
							Debug.WriteLine(" retrieving "+thiskey);
						}
						string treemap = bpt[thiskey];
						if (!treemap.Equals(thisvalue)) 
						{
							throw new ApplicationException("key "+thiskey+" maps to "+treemap+" but should map to "+thisvalue);
						}
						//allkeys.Add(thiskey);
					}
					string currentkey = bpt.FirstKey();
					Hashtable visited = new Hashtable();
					//allkeys.Sort();
					while (currentkey!=null) 
					{
						if (visited.ContainsKey(currentkey)) 
						{
							throw new ApplicationException("key visited twice in traversal "+currentkey);
						}
						visited[currentkey] = currentkey;
						if (!allmaps.ContainsKey(currentkey)) 
						{
							throw new ApplicationException("key found in tree but not in hash table "+currentkey);
						}
						currentkey = bpt.NextKey(currentkey);
					}
				}
			}
		}
		public static void xTest() 
		{
			Console.WriteLine("TESTING XBPLUSTREE");
			System.IO.Stream treefile=null, blockfile=null;
			BplusDotNet.xBplusTree bpt = getXTree(null, ref treefile, ref blockfile);
			Hashtable allmaps = new Hashtable();
			for (int i=0; i<10; i++) 
			{
				Console.WriteLine("Pass "+i+" of 10");
				bpt.SetFootPrintLimit(16-i);
				for (int j=0; j<30; j++) 
				{
					Hashtable record = new Hashtable();
					for (int k=0; k<30; k++) 
					{
						string thiskey = xkeyMaker(i,j,k);
						string thisvalue = ValueMaker(j,k,i);
						record[thiskey] = thisvalue;
						bpt[thiskey] = thisvalue;
					}
					if ((j%3)==1) 
					{
						bpt.Recover(false);
					}
					if ( ((i+j)%2) == 1 ) 
					{
						bpt.Commit();
						bpt.Abort();  // should have no effect
						bpt.Commit();  // ditto
						if ( (i+j)%5 < 2) 
						{
							//Debug.WriteLine(bpt.toHtml());
							foreach (DictionaryEntry d in record) 
							{
								string thiskey = (string) d.Key;
								bpt.RemoveKey(thiskey);
								if (allmaps.ContainsKey(thiskey)) 
								{
									allmaps.Remove(thiskey);
								}
							}
							//Debug.WriteLine(bpt.toHtml());
							bpt.Commit();
							//return;
						} 
						else 
						{
							foreach (DictionaryEntry d in record) 
							{
								allmaps[d.Key] = d.Value;
							}
						}
					} 
					else 
					{
						bpt.Abort();
					}
					if ((j%4)==2) 
					{
						bpt = getXTree(bpt, ref treefile, ref blockfile);
					}
					// now check the structure
					ArrayList allkeys = new ArrayList();
					foreach (DictionaryEntry d in allmaps) 
					{
						string thiskey = (string)d.Key;
						string thisvalue = (string)d.Value;
						string treemap = bpt[thiskey];
						if (!treemap.Equals(thisvalue)) 
						{
							throw new ApplicationException("key "+thiskey+" maps to "+treemap+" but should map to "+thisvalue);
						}
						allkeys.Add(thiskey);
					}
					string currentkey = bpt.FirstKey();
					allkeys.Sort();
					foreach (object thing in allkeys) 
					{
						//Debug.WriteLine("currentkey = "+currentkey);
						string recordedkey = (string) thing;
						if (currentkey==null) 
						{
							throw new ApplicationException("end of keys found when expecting "+recordedkey);
						}
						if (!currentkey.Equals(recordedkey)) 
						{
							Debug.WriteLine(bpt.toHtml());
							throw new ApplicationException("key "+currentkey+" found where expecting "+recordedkey);
						}
						currentkey = bpt.NextKey(currentkey);
					}
					if (currentkey!=null) 
					{
						throw new ApplicationException("found "+currentkey+" when expecting end of keys");
					}
				}
			}
		}

		public static void sTest() 
		{
			Console.WriteLine("TESTING SERIALIZEDTREE");
			System.IO.Stream treefile=null, blockfile=null;
			BplusDotNet.SerializedTree bpt = getsTree(null, ref treefile, ref blockfile);
			Hashtable allmaps = new Hashtable();
			for (int i=0; i<10; i++) 
			{
				Console.WriteLine("Pass "+i+" of 10");
				bpt.SetFootPrintLimit(16-i);
				for (int j=0; j<30; j++) 
				{
					Hashtable record = new Hashtable();
					for (int k=0; k<30; k++) 
					{
						string thiskey = keyMaker(i,j,k);
						SerializableThing thisvalue = new SerializableThing(j,k,i);
						record[thiskey] = thisvalue;
						bpt[thiskey] = thisvalue;
					}
					if ((j%3)==1) 
					{
						bpt.Recover(false);
					}
					if ( ((i+j)%2) == 1 ) 
					{
						bpt.Commit();
						bpt.Abort();  // should have no effect
						bpt.Commit();  // ditto
						if ( (i+j)%5 < 2) 
						{
							//Debug.WriteLine(bpt.toHtml());
							foreach (DictionaryEntry d in record) 
							{
								string thiskey = (string) d.Key;
								bpt.RemoveKey(thiskey);
								if (allmaps.ContainsKey(thiskey)) 
								{
									allmaps.Remove(thiskey);
								}
							}
							//Debug.WriteLine(bpt.toHtml());
							bpt.Commit();
							//return;
						} 
						else 
						{
							foreach (DictionaryEntry d in record) 
							{
								allmaps[d.Key] = d.Value;
							}
						}
					} 
					else 
					{
						bpt.Abort();
					}
					if ((j%4)==2) 
					{
						bpt = getsTree(bpt, ref treefile, ref blockfile);
					}
					// now check the structure
					ArrayList allkeys = new ArrayList();
					foreach (DictionaryEntry d in allmaps) 
					{
						string thiskey = (string)d.Key;
						SerializableThing thisvalue = (SerializableThing)d.Value;
						SerializableThing treemap = (SerializableThing)bpt[thiskey];
						if (!treemap.Equals(thisvalue)) 
						{
							throw new ApplicationException("key "+thiskey+" maps to "+treemap+" but should map to "+thisvalue);
						}
						allkeys.Add(thiskey);
					}
					string currentkey = bpt.FirstKey();
					allkeys.Sort();
					foreach (object thing in allkeys) 
					{
						string recordedkey = (string) thing;
						if (currentkey==null) 
						{
							throw new ApplicationException("end of keys found when expecting "+recordedkey);
						}
						if (!currentkey.Equals(recordedkey)) 
						{
							//Debug.WriteLine(bpt.toHtml());
							throw new ApplicationException("key "+currentkey+" found where expecting "+recordedkey);
						}
						currentkey = bpt.NextKey(currentkey);
					}
					if (currentkey!=null) 
					{
						throw new ApplicationException("found "+currentkey+" when expecting end of keys");
					}
				}
			}
		}
		public static void Test() 
		{
			Console.WriteLine("TESTING BPLUSTREE");
			System.IO.Stream treefile=null, blockfile=null;
			BplusDotNet.BplusTree bpt = getTree(null, ref treefile, ref blockfile);
			Hashtable allmaps = new Hashtable();
			for (int i=0; i<10; i++) 
			{
				Console.WriteLine("Pass "+i+" of 10");
				bpt.SetFootPrintLimit(16-i);
				for (int j=0; j<30; j++) 
				{
					Hashtable record = new Hashtable();
					for (int k=0; k<30; k++) 
					{
						string thiskey = keyMaker(i,j,k);
						string thisvalue = ValueMaker(j,k,i);
						record[thiskey] = thisvalue;
						bpt[thiskey] = thisvalue;
					}
					if ((j%3)==1) 
					{
						bpt.Recover(false);
					}
					if ( ((i+j)%2) == 1 ) 
					{
						bpt.Commit();
						bpt.Abort();  // should have no effect
						bpt.Commit();  // ditto
						if ( (i+j)%5 < 2) 
						{
							//Debug.WriteLine(bpt.toHtml());
							foreach (DictionaryEntry d in record) 
							{
								string thiskey = (string) d.Key;
								bpt.RemoveKey(thiskey);
								if (allmaps.ContainsKey(thiskey)) 
								{
									allmaps.Remove(thiskey);
								}
							}
							//Debug.WriteLine(bpt.toHtml());
							bpt.Commit();
							//return;
						} 
						else 
						{
							foreach (DictionaryEntry d in record) 
							{
								allmaps[d.Key] = d.Value;
							}
						}
					} 
					else 
					{
						bpt.Abort();
					}
					if ((j%4)==2) 
					{
						bpt = getTree(bpt, ref treefile, ref blockfile);
					}
					// now check the structure
					bool ReadOnly = ((i+j)%7)<2;
					if (ReadOnly) 
					{
						bpt = getTree(bpt, ref treefile, ref blockfile, true);
					}
					ArrayList allkeys = new ArrayList();
					foreach (DictionaryEntry d in allmaps) 
					{
						string thiskey = (string)d.Key;
						string thisvalue = (string)d.Value;
						string treemap = bpt[thiskey];
						if (!treemap.Equals(thisvalue)) 
						{
							throw new ApplicationException("key "+thiskey+" maps to "+treemap+" but should map to "+thisvalue);
						}
						allkeys.Add(thiskey);
					}
					string currentkey = bpt.FirstKey();
					allkeys.Sort();
					foreach (object thing in allkeys) 
					{
						string recordedkey = (string) thing;
						if (currentkey==null) 
						{
							throw new ApplicationException("end of keys found when expecting "+recordedkey);
						}
						if (!currentkey.Equals(recordedkey)) 
						{
							Debug.WriteLine(bpt.toHtml());
							throw new ApplicationException("key "+currentkey+" found where expecting "+recordedkey);
						}
						currentkey = bpt.NextKey(currentkey);
					}
					if (currentkey!=null) 
					{
						throw new ApplicationException("found "+currentkey+" when expecting end of keys");
					}
					// set up bpt for modification again...
					if (ReadOnly) 
					{
						bpt = getTree(bpt, ref treefile, ref blockfile, false);
					}
				}
			}
		}
		public static BplusDotNet.BplusTree getTree(BplusDotNet.BplusTree bpt, ref System.IO.Stream treefile, ref System.IO.Stream blockfile) 
		{
			return getTree(bpt, ref treefile, ref blockfile, false);
		}

		public static BplusDotNet.BplusTree getTree(BplusDotNet.BplusTree bpt, ref System.IO.Stream treefile, ref System.IO.Stream blockfile,
			bool ReadOnly) 
		{
			int CultureId = System.Globalization.CultureInfo.InvariantCulture.LCID;
			if (tempdirectory!=null) 
			{
				// allocate in filesystem
				string treename = System.IO.Path.Combine(tempdirectory, "BPDNtree.dat");
				string blockname = System.IO.Path.Combine(tempdirectory, "BPDNblock.dat");
				treefile = null;
				blockfile = null;
				if (bpt == null) 
				{
					// allocate new
					if (System.IO.File.Exists(treename)) 
					{
						System.IO.File.Delete(treename);
					}
					if (System.IO.File.Exists(blockname)) 
					{
						System.IO.File.Delete(blockname);
					}
					bpt = BplusDotNet.BplusTree.Initialize(treename, blockname, keylength, CultureId, nodesize, buffersize);
				} 
				else 
				{
					// reopen old
					bpt.Shutdown();
					if (ReadOnly) 
					{
						bpt = BplusDotNet.BplusTree.ReadOnly(treename, blockname);
					} 
					else 
					{
						bpt = BplusDotNet.BplusTree.ReOpen(treename, blockname);
					}
				}
			} 
			else 
			{
				// allocate in memory
				if (bpt==null) 
				{
					// allocate new
					treefile = new System.IO.MemoryStream();
					blockfile = new System.IO.MemoryStream();;
					bpt = BplusDotNet.BplusTree.Initialize(treefile, blockfile, keylength, CultureId, nodesize, buffersize);
				} 
				else 
				{
					// reopen
					bpt = BplusDotNet.BplusTree.ReOpen(treefile, blockfile);
				}
			}
			return bpt;
		}
		public static BplusDotNet.SerializedTree getsTree(BplusDotNet.SerializedTree bpts, ref System.IO.Stream treefile, ref System.IO.Stream blockfile) 
		{
			int CultureId = System.Globalization.CultureInfo.InvariantCulture.LCID;
			BplusDotNet.IByteTree bpt = null;
			if (tempdirectory!=null) 
			{
				// allocate in filesystem
				string treename = System.IO.Path.Combine(tempdirectory, "BPDNtreeS.dat");
				string blockname = System.IO.Path.Combine(tempdirectory, "BPDNblockS.dat");
				treefile = null;
				blockfile = null;
				if (bpts == null) 
				{
					// allocate new
					if (System.IO.File.Exists(treename)) 
					{
						System.IO.File.Delete(treename);
					}
					if (System.IO.File.Exists(blockname)) 
					{
						System.IO.File.Delete(blockname);
					}
					bpt = BplusDotNet.xBplusTreeBytes.Initialize(treename, blockname, prefixlength, CultureId, nodesize, buffersize);
				} 
				else 
				{
					// reopen old
					bpts.Shutdown();
					bpt = BplusDotNet.xBplusTreeBytes.ReOpen(treename, blockname);
				}
			} 
			else 
			{
				// allocate in memory
				if (bpts==null) 
				{
					// allocate new
					treefile = new System.IO.MemoryStream();
					blockfile = new System.IO.MemoryStream();;
					bpt = BplusDotNet.xBplusTreeBytes.Initialize(treefile, blockfile, prefixlength, CultureId, nodesize, buffersize);
				} 
				else 
				{
					// reopen
					bpt = BplusDotNet.xBplusTreeBytes.ReOpen(treefile, blockfile);
				}
			}
			return new BplusDotNet.SerializedTree(bpt);
		}
		public static BplusDotNet.hBplusTree getHTree(BplusDotNet.hBplusTree bpt, ref System.IO.Stream treefile, ref System.IO.Stream blockfile) 
		{
			int CultureId = System.Globalization.CultureInfo.InvariantCulture.LCID;
			if (tempdirectory!=null) 
			{
				// allocate in filesystem
				string treename = System.IO.Path.Combine(tempdirectory, "BPDNtreeH.dat");
				string blockname = System.IO.Path.Combine(tempdirectory, "BPDNblockH.dat");
				treefile = null;
				blockfile = null;
				if (bpt == null) 
				{
					// allocate new
					if (System.IO.File.Exists(treename)) 
					{
						System.IO.File.Delete(treename);
					}
					if (System.IO.File.Exists(blockname)) 
					{
						System.IO.File.Delete(blockname);
					}
					bpt = BplusDotNet.hBplusTree.Initialize(treename, blockname, prefixlength, CultureId, nodesize, buffersize);
				} 
				else 
				{
					// reopen old
					bpt.Shutdown();
					bpt = BplusDotNet.hBplusTree.ReOpen(treename, blockname);
				}
			} 
			else 
			{
				// allocate in memory
				if (bpt==null) 
				{
					// allocate new
					treefile = new System.IO.MemoryStream();
					blockfile = new System.IO.MemoryStream();;
					bpt = BplusDotNet.hBplusTree.Initialize(treefile, blockfile, prefixlength, CultureId, nodesize, buffersize);
				} 
				else 
				{
					// reopen
					bpt = BplusDotNet.hBplusTree.ReOpen(treefile, blockfile);
				}
			}
			return bpt;
		}
		public static BplusDotNet.xBplusTree getXTree(BplusDotNet.xBplusTree bpt, ref System.IO.Stream treefile, ref System.IO.Stream blockfile) 
		{
			int CultureId = System.Globalization.CultureInfo.InvariantCulture.LCID;
			if (tempdirectory!=null) 
			{
				// allocate in filesystem
				string treename = System.IO.Path.Combine(tempdirectory, "BPDNtreeX.dat");
				string blockname = System.IO.Path.Combine(tempdirectory, "BPDNblockX.dat");
				treefile = null;
				blockfile = null;
				if (bpt == null) 
				{
					// allocate new
					if (System.IO.File.Exists(treename)) 
					{
						System.IO.File.Delete(treename);
					}
					if (System.IO.File.Exists(blockname)) 
					{
						System.IO.File.Delete(blockname);
					}
					bpt = BplusDotNet.xBplusTree.Initialize(treename, blockname, prefixlength, CultureId, nodesize, buffersize);
				} 
				else 
				{
					// reopen old
					bpt.Shutdown();
					bpt = BplusDotNet.xBplusTree.ReOpen(treename, blockname);
				}
			} 
			else 
			{
				// allocate in memory
				if (bpt==null) 
				{
					// allocate new
					treefile = new System.IO.MemoryStream();
					blockfile = new System.IO.MemoryStream();;
					bpt = BplusDotNet.xBplusTree.Initialize(treefile, blockfile, keylength, CultureId, nodesize, buffersize);
				} 
				else 
				{
					// reopen
					bpt = BplusDotNet.xBplusTree.ReOpen(treefile, blockfile);
				}
			}
			bpt.LimitBucketSize(bucketsizelimit);
			return bpt;
		}
		public static void bplustreetest() 
		{
			allinserts = new Hashtable();
			System.IO.Stream treestream = new System.IO.MemoryStream();
			System.IO.Stream blockstream = new System.IO.MemoryStream();
			int KeyLength = 15;
			BplusDotNet.BplusTree bpt = BplusDotNet.BplusTree.Initialize(treestream, blockstream, KeyLength);
			bpt["this"] = "that";
			bpt["this2"] = "that";
			bpt["a"] = "xxx";
			bpt["c"] = "delete me";
			bpt.Recover(true);
			bpt.Commit();
			bpt["b"] = "yyy";
			bpt["a"] = "BAD VALUE";
			bpt.Abort();
			bpt.RemoveKey("c");
			bpt.Recover(false);
			string test = bpt["this"];
			Debug.WriteLine("got "+test);
			test = bpt.FirstKey();
			while (test!=null) 
			{
				Debug.WriteLine(test+" :: "+bpt[test]);
				test = bpt.NextKey(test);
			}
		}
		public static void abort(BplusDotNet.BplusTreeLong bpt) 
		{
			Debug.WriteLine(" <h3>ABORT!</H3>");
			bpt.Abort();
			allinserts = (Hashtable) lastcommittedinserts.Clone();
			checkit(bpt);
		}
		public static void commit(BplusDotNet.BplusTreeLong bpt) 
		{
			Debug.WriteLine(" <h3>COMMIT!</H3>");
			bpt.Commit();
			lastcommittedinserts = (Hashtable)allinserts.Clone();
			checkit(bpt);
		}
		public static BplusDotNet.BplusTreeLong restart(BplusDotNet.BplusTreeLong bpt) 
		{
			Debug.WriteLine(" <h3>RESTART!</H3>");
			commit(bpt);
			return BplusDotNet.BplusTreeLong.SetupFromExistingStream(bpt.Fromfile, bpt.SeekStart);
		}
		public static void inserttest(BplusDotNet.BplusTreeLong bpt, string key, long map) 
		{
			inserttest(bpt, key, map, false);
		}
		public static void deletetest(BplusDotNet.BplusTreeLong bpt, string key, long map) 
		{
			inserttest(bpt, key, map, true);
		}
		public static void inserttest(BplusDotNet.BplusTreeLong bpt, string key, long map, bool del) 
		{
			if (del) 
			{
				Debug.WriteLine(" <h3>DELETE bpt["+key+"] = "+map+"</h3>");
				bpt.RemoveKey(key);
				allinserts.Remove(key);
			} 
			else 
			{
				Debug.WriteLine("<h3>bpt["+key+"] = "+map+"</h3>");
				bpt[key] = map;
				allinserts[key] = map;
			}
			checkit(bpt);
		}
		public static void checkit(BplusDotNet.BplusTreeLong bpt) 
		{
			Debug.WriteLine(bpt.toHtml());
			bpt.SanityCheck(true);
			ArrayList allkeys = new ArrayList();
			foreach (DictionaryEntry d in allinserts) 
			{
				allkeys.Add(d.Key);
			}
			allkeys.Sort();
			allkeys.Reverse();
			foreach (object thing in allkeys)
			{
				string thekey = (string) thing;
				long thevalue = (long) allinserts[thing];
				if (thevalue!=bpt[thekey]) 
				{
					throw new ApplicationException("no match on retrieval "+thekey+" --> "+bpt[thekey]+" not "+thevalue);
				}
			}
			allkeys.Reverse();
			string currentkey = bpt.FirstKey();
			foreach (object thing in allkeys) 
			{
				string testkey = (string) thing;
				if (currentkey==null) 
				{
					throw new ApplicationException("end of keys found when expecting "+testkey);
				}
				if (!testkey.Equals(currentkey)) 
				{
					throw new ApplicationException("when walking found "+currentkey+" when expecting "+testkey);
				}
				currentkey = bpt.NextKey(testkey);
			}
		}
		public static void BplusTreeLongTest()
		{
			Console.WriteLine("TESTING BPLUSTREELONG -- LOTS OF OUTPUT to Debug.WriteLine(...)");
			for (int nodesize=2; nodesize<6; nodesize++) 
			{
				allinserts = new Hashtable();
				System.IO.Stream mstream = new System.IO.MemoryStream();
				int keylength = 10+nodesize;
				BplusDotNet.BplusTreeLong bpt = BplusDotNet.BplusTreeLong.InitializeInStream(mstream, keylength, nodesize);
				bpt = restart(bpt);
				//bpt["d"] = 15;
				inserttest(bpt, "d", 15);
				deletetest(bpt, "d", 15);
				inserttest(bpt, "d", 15);
				inserttest(bpt, "", 99);
				bpt.SerializationCheck();
				//bpt["ab"] = 55;
				inserttest(bpt, "ab", 55);
				//bpt["b"] = -5;
				inserttest(bpt, "b", -5);
				deletetest(bpt, "b", 0);
				inserttest(bpt, "b", -5);
				//return;
				//bpt["c"] = 34;
				inserttest(bpt, "c", 34);
				//bpt["a"] = 8;
				inserttest(bpt, "a", 8);
				commit(bpt);
				Debug.WriteLine("<h1>after commit</h1>\r\n");
				Debug.WriteLine(bpt.toHtml());
				//bpt["a"] = 800;
				inserttest(bpt, "a", 800);
				//bpt["ca"]= -999;
				inserttest(bpt, "ca", -999);
				//bpt["da"]= -999;
				inserttest(bpt, "da", -999);
				//bpt["ea"]= -9991;
				inserttest(bpt, "ea", -9991);
				//bpt["aa"]= -9992;
				inserttest(bpt, "aa", -9992);
				//bpt["ba"]= -9995;
				inserttest(bpt, "ba", -9995);
				commit(bpt);
				//bpt["za"]= -9997;
				inserttest(bpt, "za", -9997);
				//bpt[" a"]= -9999;
				inserttest(bpt, " a", -9999);
				commit(bpt);
				deletetest(bpt, "d", 0);
				deletetest(bpt, "da", 0);
				deletetest(bpt, "ca", 0);
				bpt = restart(bpt);
				inserttest(bpt, "aaa", 88);
				Console.WriteLine(" now doing torture test for "+nodesize);
				Debug.WriteLine("<h1>now doing torture test for "+nodesize+"</h1>");
				if (full) 
				{
					for (int i=0; i<33; i++) 
					{
						for (int k=0; k<10; k++) 
						{
							int m = (i*5+k*23)%77;
							string s = "b"+m;
							inserttest(bpt, s, m);
							if (i%2==1 || k%3==1) 
							{
								deletetest(bpt, s, m);
							}
						}
						int j = i%3;
						if (j==0) 
						{
							abort(bpt);
						} 
						else if (j==1) 
						{
							commit(bpt);
						} 
						else 
						{
							bpt = restart(bpt);
						}
					}
				}
				commit(bpt);
				deletetest(bpt, "za", 0);
				deletetest(bpt, "ea", 0);
				deletetest(bpt, "c", 0);
				deletetest(bpt, "ba", 0);
				deletetest(bpt, "b", 0);
				deletetest(bpt, "ab", 0);
				abort(bpt);
				inserttest(bpt, "dog", 1);
				commit(bpt);
				deletetest(bpt, "dog", 1);
				inserttest(bpt, "pig", 2);
				abort(bpt);
				inserttest(bpt, "cat", 3);
				bpt.Recover(true);
			}
		}
		public static String CompatKey(int i, int j, int k, int l) 
		{
			String seed = "i="+i+" j="+j+" k="+k+" ";
			String result = seed;
//			for (int ii=0; ii<l; ii++) 
//			{
//				result += seed;
//			}
			return ""+l+result;
		}
		public static String CompatValue(int i, int j, int k, int l) 
		{
			String result = CompatKey(k,j,l,i)+CompatKey(l,k,j,i);
			return result+result;
		}
		public static void CompatTest() 
		{
			if (tempdirectory==null) 
			{
				Console.WriteLine(" compatibility test requires temp directory to be defined: please edit test source file");
				return;
			}
			string myTreeFileName = tempdirectory+"\\CsharpTree.dat";
			string myBlocksFileName = tempdirectory+"\\CsharpBlocks.dat";
			string otherTreeFileName = tempdirectory+"\\JavaTree.dat";
			string otherBlocksFileName = tempdirectory+"\\JavaBlocks.dat";
			Hashtable map = new Hashtable();
			Console.WriteLine(" creating "+myTreeFileName+" and "+myBlocksFileName);
			if (System.IO.File.Exists(myTreeFileName))  
			{
				Console.WriteLine(" deleting existing files");
				System.IO.File.Delete(myTreeFileName);
				System.IO.File.Delete(myBlocksFileName);
			}
			BplusDotNet.hBplusTree myTree = BplusDotNet.hBplusTree.Initialize(myTreeFileName, myBlocksFileName, 6);
			for (int i=0; i<10; i++) 
			{
				
				Console.WriteLine(" "+i);
				for (int j=0; j<10; j++) 
				{
					for (int k=0; k<10; k++) 
					{
						for (int l=0; l<10; l++) 
						{
							String TheKey = CompatKey(i,j,k,l);
							String TheValue = CompatValue(i,j,k,l);
							map[TheKey] = TheValue;
							myTree[TheKey] = TheValue;
						}
					}
				}
			}
			myTree.Commit();
			myTree.Shutdown();
			Console.WriteLine(" trying to test "+otherTreeFileName+" and "+otherBlocksFileName);
			if (!System.IO.File.Exists(otherTreeFileName))  
			{
				Console.WriteLine(" file not created yet :(");
				//return;
			} 
			else 
			{
				int count = 0;
				BplusDotNet.hBplusTree otherTree = BplusDotNet.hBplusTree.ReadOnly(otherTreeFileName, otherBlocksFileName);
				foreach (DictionaryEntry D in map) 
				{
					if ( (count%1000)==1) 
					{
						Console.WriteLine(" ... "+count);
					}
					String TheKey = (String) D.Key;
					String TheValue = (String) D.Value;
					String OtherValue = otherTree[TheKey];
					if (!OtherValue.Equals(TheValue) )
					{
						throw new Exception(" Values don't match "+TheValue+" "+OtherValue);
					}
					count++;
				}
			}
			otherTreeFileName = tempdirectory+"\\PyTree.dat";
			otherBlocksFileName = tempdirectory+"\\PyBlocks.dat";
			Console.WriteLine(" trying to test "+otherTreeFileName+" and "+otherBlocksFileName);
			if (!System.IO.File.Exists(otherTreeFileName))  
			{
				Console.WriteLine(" file not created yet :(");
				//return;
			} 
			else 
			{
				int count = 0;
				BplusDotNet.hBplusTree otherTree = BplusDotNet.hBplusTree.ReadOnly(otherTreeFileName, otherBlocksFileName);
				foreach (DictionaryEntry D in map) 
				{
					if ( (count%1000)==1) 
					{
						Console.WriteLine(" ... "+count);
					}
					String TheKey = (String) D.Key;
					String TheValue = (String) D.Value;
					String OtherValue = otherTree[TheKey];
					if (!OtherValue.Equals(TheValue) )
					{
						throw new Exception(" Values don't match "+TheValue+" "+OtherValue);
					}
					count++;
				}
			}
			Console.WriteLine(" compatibility test ok");
		}
		public static void LinkedFileTest() 
		{
			Console.WriteLine("TESTING LINKED FILE");
			System.IO.Stream mstream = new System.IO.MemoryStream();
			// make a bunch of sample data
			int asize = 200;
			//int asize = 2;
			int maxsizing = 53;
			int prime = 17;
			int buffersize = 33;
			string seedData = "a wop bop a loo bop";
			byte[][] stuff = new byte[asize][];
			for (int i=0; i<asize; i++) 
			{
				stuff[i] = makeSampleData(seedData, (i*prime)%maxsizing);
			}
			// store them off
			BplusDotNet.LinkedFile lf = BplusDotNet.LinkedFile.InitializeLinkedFileInStream(mstream, buffersize, prime);
			lf.checkStructure();
			long[] seeks = new long[asize];
			for (int i=0; i<asize; i++) 
			{
				seeks[i] = lf.StoreNewChunk(stuff[i], 0, stuff[i].Length);
				// allocated it again and delete it off to mix things up...
				long dummy = lf.StoreNewChunk(stuff[i], 0, stuff[i].Length);
				lf.ReleaseBuffers(dummy);
				lf.checkStructure();
			}
			// delete the last one
			lf.ReleaseBuffers(seeks[asize-1]);
			lf.checkStructure();
			lf.Flush();
			// create new handle
			lf = BplusDotNet.LinkedFile.SetupFromExistingStream(mstream, prime);
			// read them back and check (except for last)
			for (int i=0; i<asize-1; i++) 
			{
				byte[] retrieved = lf.GetChunk(seeks[i]);
				testByteArrays(retrieved, stuff[i]);
				// delete every so often
				if (i%prime==1) 
				{
					lf.ReleaseBuffers(seeks[i]);
					lf.checkStructure();
				}
			}
			lf.checkStructure();
			Debug.WriteLine("");
			Debug.WriteLine("linked file tests ok");
		}
		public static byte[] makeSampleData(string testdata, int sizing) 
		{
			if (testdata.Length<1 || sizing<1) 
			{
				return new byte[0];
			}
			System.Text.StringBuilder sb = new System.Text.StringBuilder();
			for (int i=0; i<sizing; i++)
			{
				char c = testdata[i % testdata.Length];
				sb.Append(testdata);
				sb.Append(c);
			}
			string result = sb.ToString();
			return System.Text.UTF8Encoding.ASCII.GetBytes(result);
		}
		public static void testBufferFile() 
		{
			Console.WriteLine("TESTING BUFFERFILE");
			int buffersize = 17;
			int writesize = 10;
			System.IO.Stream mstream = new System.IO.MemoryStream();
			int offset = 55;
			BplusDotNet.BufferFile bf = BplusDotNet.BufferFile.InitializeBufferFileInStream(mstream, buffersize, offset);
			byte[] testheader = bf.MakeHeader();
			byte[] inputarray = makeSampleData("THIS IS SOME sample data off the cuff...", 100);
			byte[] outputarray = new byte[inputarray.Length];
			int position = 0;
			// shove in the testdata in reverse order
			for (int i=inputarray.Length; i>writesize; i-=writesize) 
			{
				Debug.Write(" "+position);
				//Console.Write(" "+position);
				bf.SetBuffer(position, inputarray, i-writesize, writesize);
				position++;
			}
			bf.SetBuffer(position, inputarray, 0, writesize);
			// extract it again
			bf = BplusDotNet.BufferFile.SetupFromExistingStream(mstream, offset);
			position = 0;
			Debug.WriteLine("");
			//Console.WriteLine("");
			for (int i=inputarray.Length; i>writesize; i-=writesize) 
			{
				Debug.Write(" "+position);
				//Console.Write(" "+position);
				bf.GetBuffer(position, outputarray, i-writesize, writesize);
				position++;
			}
			bf.GetBuffer(position, outputarray, 0, writesize);
			testByteArrays(inputarray, outputarray);
			Debug.WriteLine("");
			Debug.WriteLine(" buffer file test ok");
		}
		public static void testByteArrays(byte[] a, byte[] b) 
		{
			if (a.Length!=b.Length) 
			{
				throw new Exception("array lengths don't match "+a.Length+" "+b.Length);
			}
			for (int i=0; i<b.Length; i++) 
			{
				if (a[i]!=b[i]) 
				{
					throw new Exception("first error at "+i+" "+a[i]+" "+b[i]);
				}
			}
		}
		public static void intTests() 
		{
			int bsize = 13;
			byte[] buffer = new byte[bsize];
			int[] ints = {1, 566, -55, 32888, 4201010, 87878, -8989898};
			int index = 99;
			foreach (int theInt in ints) 
			{
				index = Math.Abs(index) % (bsize-4);
				BplusDotNet.BufferFile.Store(theInt, buffer, index);
				int otherInt = BplusDotNet.BufferFile.Retrieve(buffer, index);
				if (theInt!=otherInt) 
				{
					throw new Exception("encode/decode int failed "+theInt+"!="+otherInt);
				}
				index = (index+theInt);
			}
			Debug.WriteLine("encode/decode of ints ok");
		}
		public static void shortTests() 
		{
			int bsize = 13;
			byte[] buffer = new byte[bsize];
			short[] shorts = {1, 566, -32766, 32, 32755, 80, -8989};
			int index = 99;
			foreach (short theInt in shorts) 
			{
				index = Math.Abs(index) % (bsize-4);
				BplusDotNet.BufferFile.Store(theInt, buffer, index);
				short otherInt = BplusDotNet.BufferFile.RetrieveShort(buffer, index);
				if (theInt!=otherInt) 
				{
					throw new Exception("encode/decode int failed "+theInt+"!="+otherInt);
				}
				index = (index+theInt);
			}
			Debug.WriteLine("encode/decode of longs ok");
		}
		public static void longTests() 
		{
			int bsize = 17;
			byte[] buffer = new byte[bsize];
			long[] longs = {1, 566, -55, 32888, 4201010, 87878, -8989898, 0xefaefabbccddee, -0xefaefabbccddee};
			int index = 99;
			foreach (long theLong in longs) 
			{
				index = Math.Abs(index) % (bsize-8);
				BplusDotNet.BufferFile.Store(theLong, buffer, index);
				long otherLong = BplusDotNet.BufferFile.RetrieveLong(buffer, index);
				if (theLong!=otherLong) 
				{
					throw new Exception("encode/decode int failed "+theLong+"!="+otherLong);
				}
				index = (index+((int)(theLong&0xffffff)));
			}
			Debug.WriteLine("encode/decode of longs ok");
		}
		public static void byteStringTest() 
		{
			byte[] testbytes = new byte[128];
			for (byte i=0; i<128; i++) 
			{
				testbytes[i] = i;
			}
			string teststring = BplusDotNet.BplusTree.BytesToString(testbytes);
			if (teststring.Length!=128) 
			{
				throw new Exception("test string changed length "+teststring.Length);
			}
			testbytes = BplusDotNet.BplusTree.StringToBytes(teststring);
			if (testbytes.Length!=128)
			{
				throw new Exception("test string changed length "+teststring.Length);
			}
			//string test = BplusDotNet.hBplusTreeBytes.PrefixForByteCount1("thisThing", 5);
			//Debug.WriteLine("hash of thisThing is for len 5 '"+test+"'");
		}
	}
}
