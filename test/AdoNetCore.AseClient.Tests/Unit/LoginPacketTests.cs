using System;
using System.IO;

using System.Text;
using AdoNetCore.AseClient.Internal;
using AdoNetCore.AseClient.Packet;
using AdoNetCore.AseClient.Token;
using NUnit.Framework;

namespace AdoNetCore.AseClient.Tests.Unit
{
    [TestFixture(Explicit = true, Reason = "The capability bits of this data provider have diverged from the SAP provider.")]
    [Category("quick")]
    public class LoginPacketTests
    {
        /*
         * For reference, here's a sample of what the .net 4 driver spits out (with the 8-byte header for each chunk)
0000   02 00 02 00 00 00 00 00 43 4f 4d 50 55 54 45 52  ........COMPUTER
0010   4e 41 4d 45 00 00 00 00 00 00 00 00 00 00 00 00  NAME............
0020   00 00 00 00 00 00 0c 55 53 45 52 4e 41 4d 45 00  .......USERNAME.
0030   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0040   00 00 00 00 00 08 50 41 53 53 57 4f 52 44 00 00  ......PASSWORD..
0050   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0060   00 00 00 00 08 39 33 36 30 00 00 00 00 00 00 00  .....9360.......
0070   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0080   00 00 00 04 03 01 06 0a 09 01 00 00 00 00 00 00  ................
0090   00 00 00 00 41 50 50 4e 41 4d 45 00 00 00 00 00  ....APPNAME.....
00a0   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
00b0   00 00 07 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
00c0   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
00d0   00 00 00 08 50 41 53 53 57 4f 52 44 00 00 00 00  ....PASSWORD....
00e0   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
00f0   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0100   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0110   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0120   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0130   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0140   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0150   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0160   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0170   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0180   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0190   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
01a0   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
01b0   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
01c0   00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
01d0   00 0a 05 00 00 00 41 44 4f 2e 4e 45 54 00 00 00  ......ADO.NET...
01e0   07 0f 07 00 0d 00 0d 11 75 73 5f 65 6e 67 6c 69  ........us_engli
01f0   73 68 00 00 00 00 00 00 00 00 00 00 00 00 00 00  sh..............

0000   02 01 00 6b 00 00 00 00 00 00 00 00 00 00 0a 01  ...k............
0010   00 00 00 00 08 00 00 00 00 00 00 00 00 69 73 6f  .............iso
0020   5f 31 00 00 00 00 00 00 00 00 00 00 00 00 00 00  _1..............
0030   00 00 00 00 00 00 00 00 00 00 00 05 01 35 31 32  .............512
0040   00 00 00 03 00 00 00 00 e2 20 00 01 0e 01 ef ff  ......... ......
0050   69 b7 fd ff af 65 41 ff ff ff d6 02 0e 00 00 00  i....eA.........
0060   00 00 88 40 00 01 02 48 00 00 00                 ...@...H...
         */
        [Test]
        public void Matches_AdoNet4_Output()
        {
            using (var ms = new MemoryStream())
            {
                new LoginPacket(
                    "COMPUTERNAME",
                    "USERNAME",
                    "PASSWORD",
                    "9360",
                    "APPNAME",
                    "",
                    "us_english",
                    "iso_1",
                    "ADO.NET",
                    512,
                    new ClientCapabilityToken(),
                    false
                ).Write(
                    ms,
                    new DbEnvironment
                    {
                        Encoding = Encoding.ASCII
                    }
                );

                ms.Seek(0, SeekOrigin.Begin);

                var data = ms.ToArray();

                Console.WriteLine(HexDump.Dump(data));
            }
        }
    }
}
