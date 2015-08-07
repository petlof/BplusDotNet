using System;
using System.IO;

namespace BplusDotNet
{
	/// <summary>
	/// Provides an indexed object which maps to buffers in an underlying file object
	/// </summary>
	public class BufferFile
	{
	    readonly Stream m_fromfile;
	    readonly int m_headersize;
		// this should really be a read only property
		public int Buffersize;
	    readonly long m_seekStart;
		//byte[] header;
		public static byte[] Headerprefix = { 98, 112, 78, 98, 102 };
		public const byte Version = 0;
		public const int Intstorage = 4;
		public const int Longstorage = 8;
		public const int Shortstorage = 2;
		public static int Minbuffersize = 16;
		public BufferFile(Stream fromfile, int buffersize, long seekStart)
		{
			m_seekStart = seekStart;
			m_fromfile = fromfile;
			Buffersize = buffersize;
			m_headersize = Headerprefix.Length + Intstorage + 1; // +version byte+4 bytes for buffersize
			SanityCheck();
		}
		public BufferFile(Stream fromfile, int buffersize):
			this(fromfile, buffersize, 0) 
		{
			// just start seek at 0
		}
		public static BufferFile SetupFromExistingStream(Stream fromfile) 
		{
			return SetupFromExistingStream(fromfile, 0);
		}
		public static BufferFile SetupFromExistingStream(Stream fromfile, long startSeek) 
		{
			var result = new BufferFile(fromfile, 100, startSeek); // dummy buffer size for now
			result.ReadHeader();
			return result;
		}
		public static BufferFile InitializeBufferFileInStream(Stream fromfile, int buffersize) 
		{
			return InitializeBufferFileInStream(fromfile, buffersize, 0);
		}
		public static BufferFile InitializeBufferFileInStream(Stream fromfile, int buffersize, long startSeek) 
		{
			var result = new BufferFile(fromfile, buffersize, startSeek);
			result.SetHeader();
			return result;
		}
		void SanityCheck() 
		{
			if (Buffersize<Minbuffersize) 
			{
				throw new BufferFileException("buffer size too small "+Buffersize);
			}
			if (m_seekStart<0) 
			{
				throw new BufferFileException("can't start at negative position "+m_seekStart);
			}
		}
		public void GetBuffer(long buffernumber, byte[] toArray, int startingAt, int length)
		{
			if (buffernumber>=NextBufferNumber()) 
			{
				throw new BufferFileException("last buffer is "+NextBufferNumber()+" not "+buffernumber);
			}
			if (length>Buffersize) 
			{
				throw new BufferFileException("buffer size too small for retrieval "+Buffersize+" need "+length);
			}
			long seekPosition = BufferSeek(buffernumber);
			m_fromfile.Seek(seekPosition, SeekOrigin.Begin);
			m_fromfile.Read(toArray, startingAt, length);
		} 
		public void SetBuffer(long buffernumber, byte[] fromArray, int startingAt, int length)
		{
			//System.Diagnostics.Debug.WriteLine("<br> setting buffer "+buffernumber);
			if (length>Buffersize) 
			{
				throw new BufferFileException("buffer size too small for assignment "+Buffersize+" need "+length);
			}
			if (buffernumber>NextBufferNumber()) 
			{
				throw new BufferFileException("cannot skip buffer numbers from "+NextBufferNumber()+" to "+buffernumber);
			}
			long seekPosition = BufferSeek(buffernumber);
			// need to fill with junk if beyond eof?
			m_fromfile.Seek(seekPosition, SeekOrigin.Begin);
			//this.fromfile.Seek(seekPosition);
			m_fromfile.Write(fromArray, startingAt, length);
		}
		void SetHeader() 
		{
			byte[] header = MakeHeader();
			m_fromfile.Seek(m_seekStart, SeekOrigin.Begin);
			m_fromfile.Write(header, 0, header.Length);
		}
		public void Flush() 
		{
			m_fromfile.Flush();
		}
		void ReadHeader() 
		{
			var header = new byte[m_headersize];
			m_fromfile.Seek(m_seekStart, SeekOrigin.Begin);
			m_fromfile.Read(header, 0, m_headersize);
			int index = 0;
			// check prefix
			foreach (byte b in Headerprefix) 
			{
				if (header[index]!=b) 
				{
					throw new BufferFileException("invalid header prefix");
				}
				index++;
			}
			// skip version (for now)
			index++;
			// read buffersize
			Buffersize = Retrieve(header, index);
			SanityCheck();
			//this.header = header;
		}
		public byte[] MakeHeader() 
		{
			var result = new byte[m_headersize];
			Headerprefix.CopyTo(result, 0);
			result[Headerprefix.Length] = Version;
			Store(Buffersize, result, Headerprefix.Length+1);
			return result;
		}
		long BufferSeek(long bufferNumber) 
		{
			if (bufferNumber<0) 
			{
				throw new BufferFileException("buffer number cannot be negative");
			}
			return m_seekStart+m_headersize+(Buffersize*bufferNumber);
		}
		public long NextBufferNumber() 
		{
			// round up the buffer number based on the current file length
			long filelength = m_fromfile.Length;
			long bufferspace = filelength-m_headersize-m_seekStart;
			long nbuffers = bufferspace/Buffersize;
			long remainder = bufferspace%Buffersize;
			if (remainder>0) 
			{
				return nbuffers+1;
			}
			return nbuffers;
		}
		// there are probably libraries for this, but whatever...
		public static void Store(int theInt, byte[] toArray, int atIndex) 
		{
			const int limit = Intstorage;
			if (atIndex+limit>toArray.Length) 
			{
				throw new BufferFileException("can't access beyond end of array");
			}
			for (int i=0; i<limit; i++) 
			{
				var thebyte = (byte) (theInt & 0xff);
				toArray[atIndex+i] = thebyte;
				theInt = theInt>>8;
			}
		}
		public static void Store(short theShort, byte[] toArray, int atIndex) 
		{
			const int limit = Shortstorage;
			int theInt = theShort;
			if (atIndex+limit>toArray.Length) 
			{
				throw new BufferFileException("can't access beyond end of array");
			}
			for (int i=0; i<limit; i++) 
			{
				var thebyte = (byte) (theInt & 0xff);
				toArray[atIndex+i] = thebyte;
				theInt = theInt>>8;
			}
		}
		public static int Retrieve(byte[] toArray, int atIndex) 
		{
			const int limit = Intstorage;
			if (atIndex+limit>toArray.Length) 
			{
				throw new BufferFileException("can't access beyond end of array");
			}
			int result = 0;
			for (int i=0; i<limit; i++) 
			{
				byte thebyte = toArray[atIndex+limit-i-1];
				result = result << 8;
				result = result | thebyte;
			}
			return result;
		}
		public static void Store(long theLong, byte[] toArray, int atIndex) 
		{
			const int limit = Longstorage;
			if (atIndex+limit>toArray.Length) 
			{
				throw new BufferFileException("can't access beyond end of array");
			}
			for (int i=0; i<limit; i++) 
			{
				var thebyte = (byte) (theLong & 0xff);
				toArray[atIndex+i] = thebyte;
				theLong = theLong>>8;
			}
		}
		public static long RetrieveLong(byte[] toArray, int atIndex) 
		{
			const int limit = Longstorage;
			if (atIndex+limit>toArray.Length) 
			{
				throw new BufferFileException("can't access beyond end of array");
			}
			long result = 0;
			for (int i=0; i<limit; i++) 
			{
				byte thebyte = toArray[atIndex+limit-i-1];
				result = result << 8;
				result = result | thebyte;
			}
			return result;
		}
		public static short RetrieveShort(byte[] toArray, int atIndex) 
		{
			const int limit = Shortstorage;
			if (atIndex+limit>toArray.Length) 
			{
				throw new BufferFileException("can't access beyond end of array");
			}
			int result = 0;
			for (int i=0; i<limit; i++) 
			{
				byte thebyte = toArray[atIndex+limit-i-1];
				result = (result << 8);
				result = result | thebyte;
			}
			return (short) result;
		}
	}
	public class BufferFileException: ApplicationException 
	{
		public BufferFileException(string message): base(message) 
		{
			// do nothing extra
		}
	}
}
