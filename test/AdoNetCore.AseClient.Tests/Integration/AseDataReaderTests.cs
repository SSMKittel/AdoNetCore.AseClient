using System;
using System.Collections.Generic;
using System.Data;
using Dapper;
using NUnit.Framework;

namespace AdoNetCore.AseClient.Tests.Integration
{
    [TestFixture]
    [Category("extra")]
    public class AseDataReaderTests
    {
        private const string AllTypesQuery =
@"SELECT 
    CAST(123 AS INT) AS [INT],
    CAST(NULL AS INT) AS [NULL_INT],
    CAST(123 AS SMALLINT) AS [SMALLINT],
    CAST(NULL AS SMALLINT) AS [NULL_SMALLINT],
    CAST(123 AS BIGINT) AS [BIGINT],
    CAST(NULL AS BIGINT) AS [NULL_BIGINT],
    CAST(123 AS TINYINT) AS [TINYINT],
    CAST(NULL AS TINYINT) AS [NULL_TINYINT],

    CAST(123 AS UNSIGNED INT) AS [UNSIGNED_INT],
    CAST(NULL AS UNSIGNED INT) AS [NULL_UNSIGNED_INT],
    CAST(123 AS UNSIGNED SMALLINT) AS [UNSIGNED_SMALLINT],
    CAST(NULL AS UNSIGNED SMALLINT) AS [NULL_UNSIGNED_SMALLINT],
    CAST(123 AS UNSIGNED BIGINT) AS [UNSIGNED_BIGINT],
    CAST(NULL AS UNSIGNED BIGINT) AS [NULL_UNSIGNED_BIGINT],
    CAST(123 AS UNSIGNED TINYINT) AS [UNSIGNED_TINYINT],
    CAST(NULL AS UNSIGNED TINYINT) AS [NULL_UNSIGNED_TINYINT],

    CAST(123.45 AS REAL) AS [REAL],
    CAST(NULL AS REAL) AS [NULL_REAL],
    CAST(123.45 AS DOUBLE PRECISION) AS [DOUBLE_PRECISION],
    CAST(NULL AS DOUBLE PRECISION) AS [NULL_DOUBLE_PRECISION],
    CAST(123.45 AS NUMERIC(18,6)) AS [NUMERIC],
    CAST(NULL AS NUMERIC(18,6)) AS [NULL_NUMERIC],

    CAST(123.4567 AS MONEY) AS [MONEY],
    CAST(NULL AS MONEY) AS [NULL_MONEY],
    CAST(123.4567 AS SMALLMONEY) AS [SMALLMONEY],
    CAST(NULL AS SMALLMONEY) AS [NULL_SMALLMONEY],

    CAST(1 AS BIT) AS [BIT],
    CAST(0Xc9f317f51cdb45ba903e82bb4bfed8ef AS BINARY(16)) AS [BINARY],
    CAST(NULL AS BINARY(16)) AS [NULL_BINARY],
    CAST(0Xc9f317f51cdb45ba903e82bb4bfed8ef AS VARBINARY(16)) AS [VARBINARY],
    CAST(NULL AS VARBINARY(16)) AS [NULL_VARBINARY],
    CAST(0Xc9f317f51cdb45ba903e82bb4bfed8ef AS IMAGE) AS [IMAGE],
    CAST(NULL AS IMAGE) AS [NULL_IMAGE],

    CAST('Hello world' AS VARCHAR) AS [VARCHAR],
    CAST(NULL AS VARCHAR) AS [NULL_VARCHAR],
    CAST('Hello world' AS CHAR) AS [CHAR],
    CAST(NULL AS CHAR) AS [NULL_CHAR],
    CAST('Hello world' AS UNIVARCHAR) AS [UNIVARCHAR],
    CAST(NULL AS UNIVARCHAR) AS [NULL_UNIVARCHAR],
    CAST('Hello world' AS UNICHAR) AS [UNICHAR],
    CAST(NULL AS UNICHAR) AS [NULL_UNICHAR],
    CAST('Hello world' AS TEXT) AS [TEXT],
    CAST(NULL AS TEXT) AS [NULL_TEXT],
    CAST('Hello world' AS UNITEXT) AS [UNITEXT],
    CAST(NULL AS UNITEXT) AS [NULL_UNITEXT],

    --CAST('Apr 15 1987 10:23:00.000000PM' AS BIGDATETIME) AS [BIGDATETIME],
    --CAST(NULL AS BIGDATETIME) AS [NULL_BIGDATETIME],
    CAST('Apr 15 1987 10:23:00.000PM' AS DATETIME) AS [DATETIME],
    CAST(NULL AS DATETIME) AS [NULL_DATETIME],
    CAST('Apr 15 1987 10:23:00PM' AS SMALLDATETIME) AS [SMALLDATETIME],
    CAST(NULL AS SMALLDATETIME) AS [NULL_SMALLDATETIME],
    CAST('Apr 15 1987' AS DATE) AS [DATE],
    CAST(NULL AS DATE) AS [NULL_DATE],

    --CAST('11:59:59.999999 PM' AS BIGTIME) AS [BIGTIME],
    --CAST(NULL AS BIGTIME) AS [NULL_BIGTIME],
    CAST('23:59:59:997' AS TIME) AS [TIME],
    CAST(NULL AS TIME) AS [NULL_TIME]
";

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetInt32_WithValue_CastSuccessfully(string aseType)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetInt32(ordinal), 123);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetInt32_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetInt32(ordinal), exceptionType);
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetInt32_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetInt32(ordinal));
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetInt16_WithValue_CastSuccessfully(string aseType)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetInt16(ordinal), 123);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetInt16_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetInt16(ordinal), exceptionType);
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetInt16_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetInt16(ordinal));
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetInt64_WithValue_CastSuccessfully(string aseType)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetInt64(ordinal), 123);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetInt64_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetInt64(ordinal), exceptionType);
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetInt64_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetInt64(ordinal));
        }


        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetUInt32_WithValue_CastSuccessfully(string aseType)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetUInt32(ordinal), 123u);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetUInt32_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetUInt32(ordinal), exceptionType);
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetUInt32_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetUInt32(ordinal));
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetUInt16_WithValue_CastSuccessfully(string aseType)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetUInt16(ordinal), 123u);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetUInt16_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetUInt16(ordinal), exceptionType);
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetUInt16_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetUInt16(ordinal));
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetUInt64_WithValue_CastSuccessfully(string aseType)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetUInt64(ordinal), 123u);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetUInt64_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetUInt64(ordinal), exceptionType);
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetUInt64_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetUInt64(ordinal));
        }

        [TestCase("BINARY")]
        [TestCase("VARBINARY")]
        [TestCase("IMAGE")]
        public void GetBytes_WithValue_CastSuccessfully(string aseType)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetBytes(ordinal, 0, new byte[16], 0, 16), 16);
        }

        [TestCase("BINARY")]
        [TestCase("VARBINARY")]
        [TestCase("IMAGE")]
        public void GetBytes_WithNullValue_ReturnsNull(string aseType)
        {
            using (var connection = new AseConnection(ConnectionStrings.Pooled))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = AllTypesQuery;

                    using (var reader = command.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        var targetFieldOrdinal = reader.GetOrdinal($"NULL_{aseType}");

                        Assert.IsTrue(reader.Read());
                        Assert.AreEqual(0, reader.GetBytes(targetFieldOrdinal, 0, new byte[16], 0, 16));
                    }
                }
            }
        }

        [TestCase("BINARY")]
        public void GetGuid_WithValue_CastSuccessfully(string aseType)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType,
                (reader, ordinal) => reader.GetGuid(ordinal),
                new Guid(new byte[] { 0xc9, 0xf3, 0x17, 0xf5, 0x1c, 0xdb, 0x45, 0xba, 0x90, 0x3e, 0x82, 0xbb, 0x4b, 0xfe, 0xd8, 0xef }));
        }

        [TestCase("BIGINT", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(InvalidCastException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("DOUBLE_PRECISION", typeof(InvalidCastException))]
        [TestCase("INT", typeof(InvalidCastException))]
        [TestCase("MONEY", typeof(InvalidCastException))]
        [TestCase("NUMERIC", typeof(InvalidCastException))]
        [TestCase("REAL", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("SMALLINT", typeof(InvalidCastException))]
        [TestCase("SMALLMONEY", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(InvalidCastException))]
        [TestCase("TINYINT", typeof(InvalidCastException))]
        [TestCase("UNICHAR", typeof(InvalidCastException))]
        [TestCase("UNITEXT", typeof(InvalidCastException))]
        [TestCase("UNIVARCHAR", typeof(InvalidCastException))]
        [TestCase("UNSIGNED_BIGINT", typeof(InvalidCastException))]
        [TestCase("UNSIGNED_INT", typeof(InvalidCastException))]
        [TestCase("UNSIGNED_SMALLINT", typeof(InvalidCastException))]
        [TestCase("UNSIGNED_TINYINT", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(InvalidCastException))]
        public void GetGuid_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetGuid(ordinal), exceptionType);
        }

        [TestCase("BINARY")]
        public void GetGuid_WithNullValue_ReturnsNull(string aseType)
        {
            GetHelper_WithNullValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetGuid(ordinal), Guid.Empty);
        }

        [TestCase("VARCHAR", "Hello world")]
        [TestCase("CHAR", "Hello world")]
        [TestCase("UNIVARCHAR", "Hello world")]
        [TestCase("UNICHAR", "Hello world")]
        [TestCase("TEXT", "Hello world")]
        [TestCase("UNITEXT", "Hello world")]
        public void GetString_WithValue_CastSuccessfully(string aseType, string expectedValue)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetString(ordinal), expectedValue);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        public void GetString_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetString(ordinal), exceptionType);
        }

        [TestCase("VARCHAR")]
        [TestCase("CHAR")]
        [TestCase("UNIVARCHAR")]
        [TestCase("UNICHAR")]
        [TestCase("TEXT")]
        [TestCase("UNITEXT")]
        public void GetString_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetString(ordinal));
        }

        [TestCase("DATE", "Apr 15 1987")]
        [TestCase("DATETIME", "Apr 15 1987 10:23:00.000PM")]
        [TestCase("SMALLDATETIME", "Apr 15 1987 10:23:00PM")]
        [TestCase("BIGDATETIME", "Apr 15 1987 10:23:00.000000PM", Ignore = "true", IgnoreReason = "BIGDATETIME is not supported yet")]
        public void GetDateTime_WithValue_CastSuccessfully(string aseType, string expectedDateTime)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetDateTime(ordinal), DateTime.Parse(expectedDateTime));
        }

        [TestCase("BIGINT", typeof(InvalidCastException))]
        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DOUBLE_PRECISION", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("INT", typeof(InvalidCastException))]
        [TestCase("MONEY", typeof(InvalidCastException))]
        [TestCase("NUMERIC", typeof(InvalidCastException))]
        [TestCase("REAL", typeof(InvalidCastException))]
        [TestCase("SMALLINT", typeof(InvalidCastException))]
        [TestCase("SMALLMONEY", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("TINYINT", typeof(InvalidCastException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("UNSIGNED_BIGINT", typeof(InvalidCastException))]
        [TestCase("UNSIGNED_INT", typeof(InvalidCastException))]
        [TestCase("UNSIGNED_SMALLINT", typeof(InvalidCastException))]
        [TestCase("UNSIGNED_TINYINT", typeof(InvalidCastException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetDateTime_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetDateTime(ordinal), exceptionType);
        }

        [TestCase("DATE")]
        [TestCase("DATETIME")]
        [TestCase("SMALLDATETIME")]
        [TestCase("BIGDATETIME", Ignore = "true", IgnoreReason = "BIGDATETIME is not supported yet")]
        public void GetDateTime_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetDateTime(ordinal));
        }

        [TestCase("TIME", "23:59:59.996")]
        [TestCase("BIGTIME", "11:59:59.999999 PM", Ignore = "true", IgnoreReason = "BIGTIME is not supported yet")]
        public void GetTimeSpan_WithValue_CastSuccessfully(string aseType, string expectedTimeSpan)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetTimeSpan(ordinal), TimeSpan.Parse(expectedTimeSpan));
        }

        [TestCase("BIGINT", typeof(InvalidCastException))]
        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(InvalidCastException))]
        [TestCase("DOUBLE_PRECISION", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("INT", typeof(InvalidCastException))]
        [TestCase("MONEY", typeof(InvalidCastException))]
        [TestCase("NUMERIC", typeof(InvalidCastException))]
        [TestCase("REAL", typeof(InvalidCastException))]
        [TestCase("SMALLINT", typeof(InvalidCastException))]
        [TestCase("SMALLMONEY", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(InvalidCastException))]
        [TestCase("TINYINT", typeof(InvalidCastException))]
        [TestCase("UNICHAR", typeof(InvalidCastException))]
        [TestCase("UNITEXT", typeof(InvalidCastException))]
        [TestCase("UNIVARCHAR", typeof(InvalidCastException))]
        [TestCase("UNSIGNED_BIGINT", typeof(InvalidCastException))]
        [TestCase("UNSIGNED_INT", typeof(InvalidCastException))]
        [TestCase("UNSIGNED_SMALLINT", typeof(InvalidCastException))]
        [TestCase("UNSIGNED_TINYINT", typeof(InvalidCastException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(InvalidCastException))]
        public void GetTimeSpan_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetTimeSpan(ordinal), exceptionType);
        }

        [TestCase("TIME")]
        [TestCase("BIGTIME", Ignore = "true", IgnoreReason = "BIGTIME is not supported yet")]
        public void GetTimeSpan_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetTimeSpan(ordinal));
        }

        [TestCase("BIT")]
        public void GetBoolean_WithValue_CastSuccessfully(string aseType)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetBoolean(ordinal), true);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetBoolean_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetBoolean(ordinal), exceptionType);
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetByte_WithValue_CastSuccessfully(string aseType)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetByte(ordinal), 123);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetByte_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetByte(ordinal), exceptionType);
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetByte_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetByte(ordinal));
        }

        [TestCase("INT", 123)]
        [TestCase("BIGINT", 123)]
        [TestCase("SMALLINT", 123)]
        [TestCase("TINYINT", 123)]
        [TestCase("UNSIGNED_INT", 123)]
        [TestCase("UNSIGNED_BIGINT", 123)]
        [TestCase("UNSIGNED_SMALLINT", 123)]
        [TestCase("UNSIGNED_TINYINT", 123)]
        [TestCase("REAL", 123.45f)]
        [TestCase("DOUBLE_PRECISION", 123.45f)]
        [TestCase("NUMERIC", 123.45f)]
        [TestCase("MONEY", 123.4567f)]
        [TestCase("SMALLMONEY", 123.4567f)]
        public void GetFloat_WithValue_CastSuccessfully(string aseType, float expectedValue)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetFloat(ordinal), expectedValue);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetFloat_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetFloat(ordinal), exceptionType);
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetFloat_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetFloat(ordinal));
        }

        [TestCase("INT", 123d)]
        [TestCase("BIGINT", 123d)]
        [TestCase("TINYINT", 123d)]
        [TestCase("SMALLINT", 123d)]
        [TestCase("UNSIGNED_INT", 123d)]
        [TestCase("UNSIGNED_BIGINT", 123d)]
        [TestCase("UNSIGNED_SMALLINT", 123d)]
        [TestCase("UNSIGNED_TINYINT", 123d)]
        [TestCase("REAL", 123.45d)]
        [TestCase("DOUBLE_PRECISION", 123.45d)]
        [TestCase("NUMERIC", 123.45d)]
        [TestCase("MONEY", 123.4567d)]
        [TestCase("SMALLMONEY", 123.4567d)]
        public void GetDouble_WithValue_CastSuccessfully(string aseType, double expectedValue)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetDouble(ordinal), expectedValue);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetDouble_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetDouble(ordinal), exceptionType);
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetDouble_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetDouble(ordinal));
        }

        [TestCase("INT", 123)]
        [TestCase("BIGINT", 123)]
        [TestCase("TINYINT", 123)]
        [TestCase("SMALLINT", 123)]
        [TestCase("UNSIGNED_INT", 123)]
        [TestCase("UNSIGNED_BIGINT", 123)]
        [TestCase("UNSIGNED_SMALLINT", 123)]
        [TestCase("UNSIGNED_TINYINT", 123)]
        [TestCase("REAL", 123.45d)]
        [TestCase("DOUBLE_PRECISION", 123.45d)]
        [TestCase("NUMERIC", 123.45d)]
        [TestCase("MONEY", 123.4567d)]
        [TestCase("SMALLMONEY", 123.4567d)]
        public void GetDecimal_WithValue_CastSuccessfully(string aseType, decimal expectedValue)
        {
            GetHelper_WithValue_TCastSuccessfully(aseType, (reader, ordinal) => reader.GetDecimal(ordinal), expectedValue);
        }

        [TestCase("BINARY", typeof(InvalidCastException))]
        [TestCase("CHAR", typeof(FormatException))]
        [TestCase("DATE", typeof(InvalidCastException))]
        [TestCase("DATETIME", typeof(InvalidCastException))]
        [TestCase("IMAGE", typeof(InvalidCastException))]
        [TestCase("SMALLDATETIME", typeof(InvalidCastException))]
        [TestCase("TEXT", typeof(FormatException))]
        [TestCase("UNICHAR", typeof(FormatException))]
        [TestCase("UNITEXT", typeof(FormatException))]
        [TestCase("UNIVARCHAR", typeof(FormatException))]
        [TestCase("VARBINARY", typeof(InvalidCastException))]
        [TestCase("VARCHAR", typeof(FormatException))]
        public void GetDecimal_WithInvalidlyTypedValue_ThrowsException(string aseType, Type exceptionType)
        {
            GetHelper_WithValue_ThrowsException(aseType, (reader, ordinal) => reader.GetDecimal(ordinal), exceptionType);
        }

        [TestCase("INT")]
        [TestCase("BIGINT")]
        [TestCase("SMALLINT")]
        [TestCase("TINYINT")]
        [TestCase("UNSIGNED_INT")]
        [TestCase("UNSIGNED_BIGINT")]
        [TestCase("UNSIGNED_SMALLINT")]
        [TestCase("UNSIGNED_TINYINT")]
        [TestCase("REAL")]
        [TestCase("DOUBLE_PRECISION")]
        [TestCase("NUMERIC")]
        [TestCase("MONEY")]
        [TestCase("SMALLMONEY")]
        public void GetDecimal_WithNullValue_ThrowsAseException(string aseType)
        {
            GetHelper_WithNullValue_ThrowsAseException(aseType, (reader, ordinal) => reader.GetDecimal(ordinal));
        }

        private void GetHelper_WithValue_TCastSuccessfully<T>(string columnName, Func<AseDataReader, int, T> testMethod, T expectedValue)
        {
            using (var connection = new AseConnection(ConnectionStrings.Pooled))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = AllTypesQuery;

                    using (var reader = command.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        var targetFieldOrdinal = reader.GetOrdinal(columnName);

                        Assert.IsTrue(reader.Read());

                        T value = testMethod(reader, targetFieldOrdinal);

                        if (expectedValue is float || expectedValue is double)
                        {
                            Assert.That(expectedValue, Is.EqualTo(value).Within(0.1));
                        }
                        else if (expectedValue is string)
                        {
                            Assert.AreEqual(expectedValue, (value as string)?.Trim());
                        }
                        else
                        {
                            Assert.AreEqual(expectedValue, value);
                        }

                        Assert.IsFalse(reader.Read());
                        Assert.IsFalse(reader.NextResult());
                    }
                }
            }
        }

        private void GetHelper_WithValue_ThrowsException<T>(string columnName, Func<AseDataReader, int, T> testMethod, Type exceptionType)
        {
            using (var connection = new AseConnection(ConnectionStrings.Pooled))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = AllTypesQuery;

                    using (var reader = command.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        var targetFieldOrdinal = reader.GetOrdinal(columnName);

                        Assert.IsTrue(reader.Read());
                        Assert.Throws(exceptionType, () => testMethod(reader, targetFieldOrdinal));
                    }
                }
            }
        }

        private void GetHelper_WithNullValue_ThrowsAseException<T>(string columnName, Func<AseDataReader, int, T> testMethod)
        {
            using (var connection = new AseConnection(ConnectionStrings.Pooled))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText = AllTypesQuery;

                    using (var reader = command.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        var targetFieldOrdinal = reader.GetOrdinal($"NULL_{columnName}");

                        Assert.IsTrue(reader.Read());

                        var ex = Assert.Throws<AseException>(() => testMethod(reader, targetFieldOrdinal));
                        Assert.AreEqual("Value in column is null", ex.Message);
                        Assert.AreEqual(30014, ex.Errors[0].MessageNumber);
                    }
                }
            }
        }

        private void GetHelper_WithNullValue_TCastSuccessfully<T>(string columnName, Func<AseDataReader, int, T> testMethod, T expectedValue)
        {
            GetHelper_WithValue_TCastSuccessfully($"NULL_{columnName}", testMethod, expectedValue);
        }

        [TestCase(CommandBehavior.Default, 5)]
        [TestCase(CommandBehavior.SingleResult, 3)]
        [TestCase(CommandBehavior.SingleRow, 1)]
        public void ExecuteReader_WithCommandBehavior_ReturnsTheCorrectNumberOfRows(CommandBehavior behavior, int expectedNumberOfRows)
        {
            using (var connection = new AseConnection(ConnectionStrings.Pooled))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandText =
 @"SELECT 1 AS MyColumn 
UNION ALL
SELECT 2
UNION ALL
SELECT 3

SELECT 4 AS MyOtherColumn 
UNION ALL
SELECT 5";

                    using (var reader = command.ExecuteReader(behavior))
                    {
                        var recordCount = 0;

                        do
                        {
                            while (reader.Read())
                            {
                                recordCount++;
                            }
                        } while (reader.NextResult());


                        Assert.AreEqual(expectedNumberOfRows, recordCount);
                    }
                }
            }
        }

        [TestCaseSource(nameof(GetFieldType_ReturnsNonNullableType_Cases))]
        public void GetFieldType_ReturnsNonNullableType(string query, Type expected)
        {
            using (var connection = new AseConnection(ConnectionStrings.Pooled))
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = query;
                    var reader = command.ExecuteReader();
                    reader.Read();
                    Assert.AreEqual(expected, reader.GetFieldType(0));
                    Assert.IsTrue(reader.IsDBNull(0));
                }
            }
        }

        public static IEnumerable<TestCaseData> GetFieldType_ReturnsNonNullableType_Cases()
        {
            //todo: implement bigdatetime support
            //yield return new TestCaseData("select convert(bigdatetime, null)", typeof(DateTime));
            yield return new TestCaseData("select convert(bigint, null)", typeof(long));
            //todo: implement bigtime support
            //yield return new TestCaseData("select convert(bigtime, null)", typeof(DateTime));
            yield return new TestCaseData("select convert(binary, null)", typeof(byte[]));
            yield return new TestCaseData("select convert(char, null)", typeof(string));
            yield return new TestCaseData("select convert(date, null)", typeof(DateTime));
            yield return new TestCaseData("select convert(datetime, null)", typeof(DateTime));
            yield return new TestCaseData("select convert(double precision, null)", typeof(double));
            yield return new TestCaseData("select convert(image, null)", typeof(byte[]));
            yield return new TestCaseData("select convert(int, null)", typeof(int));
            yield return new TestCaseData("select convert(money, null)", typeof(decimal));
            yield return new TestCaseData("select convert(numeric, null)", typeof(decimal));
            yield return new TestCaseData("select convert(real, null)", typeof(float));
            yield return new TestCaseData("select convert(smalldatetime, null)", typeof(DateTime));
            yield return new TestCaseData("select convert(smallint, null)", typeof(short));
            yield return new TestCaseData("select convert(smallmoney, null)", typeof(decimal));
            yield return new TestCaseData("select convert(text, null)", typeof(string));
            yield return new TestCaseData("select convert(time, null)", typeof(DateTime));
            yield return new TestCaseData("select convert(tinyint, null)", typeof(byte));
            yield return new TestCaseData("select convert(unichar, null)", typeof(string));
            yield return new TestCaseData("select convert(unitext, null)", typeof(string));
            yield return new TestCaseData("select convert(univarchar, null)", typeof(string));
            yield return new TestCaseData("select convert(unsigned bigint, null)", typeof(ulong));
            yield return new TestCaseData("select convert(unsigned int, null)", typeof(uint));
            yield return new TestCaseData("select convert(unsigned smallint, null)", typeof(ushort));
            yield return new TestCaseData("select convert(unsigned tinyint, null)", typeof(byte));
            yield return new TestCaseData("select convert(varbinary, null)", typeof(byte[]));
            yield return new TestCaseData("select convert(varchar, null)", typeof(string));
        }

        [Test]
        public void Issue102()
        {
            using (var connection = new AseConnection(ConnectionStrings.Pooled))
            {
                connection.Open();

                using (var command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText =
                        @"create or replace procedure issue_102

@NUM_PDV            numeric(9),
@NUM_FCT            int,
@COD_TIP_ACAO_FCT   tinyint,
@COD_INDC_REG_ATU   char(1),
@COD_SIT_FCT        char(1),
@COD_TERM_DST       char(8),
@COD_TIP_EQPM       char(3),
@IND_TCNL_CMPH      char(1),
@COD_CNL_DSTR       numeric(2),
@COD_CEL_DSTR       int,
@DTH_ASSN_CTR_POS   datetime,
@VAL_INI_ALGL       numeric(9, 2),
@COD_INDC_ISN_ALGL  char(1),
@DTH_FIM_ISN_ALGL   datetime,
@COD_INDC_ECLM      char(1),
@COD_EVN_ESPC       char(2),
@COD_TIP_LGCO       tinyint,
@COD_VDA_DGTD_POS   char(1),
@COD_INDC_PRRD      char(1),
@DTH_LIM_PRRD       datetime,
@COD_DFR_END        char(1),
@NOM_END_TERM       char(60),
@NUM_END_TERM       char(6),
@NOM_END_CPL_TERM   char(100),
@NOM_BRR_TERM       char(20),
@NOM_CID_TERM       char(50),
@NOM_EST_TERM       char(2),
@COD_CEP_TERM       char(5),
@COD_CEP_CPL_TERM   numeric(3),
@NUM_DDD_TERM       char(4),
@NUM_TEL_TERM       int,
@NOM_PES_CTTO       char(50),
@COD_TERM           char(8),
@NUM_HDSK_ESTB      int,
@COD_HRRO_SMNA_INI  numeric(1),
@COD_HRRO_SMNA_FIM  numeric(1),
@HOR_INI_FNCN       numeric(4),
@HOR_FIM_FNCN       numeric(4),
@DES_OBS_FCT        varchar(255),
@COD_FBRC_HDW       char(2),
@COD_FORN_SFTW      char(2),
@COD_INTG           char(2),
@COD_TIP_CNXO       int,
@COD_VERS_SFTW      int,
@NUM_RNPC           numeric(15),
@COD_PRPT_TERM      tinyint,
@QTD_CHCK_OUT       int,
@COD_INDC_TEM_PNPD  char(1),
@COD_OPID_ULT_ATLZ  char(8),
@NUM_ULT_SEQ_FCT    int,
@NUM_ANT            int,
@COD_TIP_NMRC       tinyint,
@TIMESTAMP          varchar(255),
@NUM_FCT_ORI        int,
@NUM_PDV_ORI        numeric(9),
@TIMESTAMP_ORI      varchar(255),
@COD_INDC_VLDC_ENDR char(1),
@COD_ACAO_CMC       tinyint,
@COD_PRRD_NEGC      tinyint,
@IND_ISN_PGMN       char(1),
@COD_CNRO           int,
@IND_SZND           char(1),
@NUM_FLCO_AMEX      numeric(16),
@NUM_FLCO_VISA      numeric(16),
@NUM_FCT_ANT        int         = null ,    -- desv12795
@COD_ORG_SLC_FCT    tinyint     = 0,
@IndTermFatrExps    char(1)     = 'N'

AS

   select 0 as cod_err, 'sucesso' as des_err , 0";

                    command.ExecuteNonQuery();
                    
                    using (var reader = connection.ExecuteReader("issue_102", new
                        {
                        NUM_PDV = 1,
                        NUM_FCT = 2,
                        COD_TIP_ACAO_FCT = 3,
                        COD_INDC_REG_ATU = 'a',
                        COD_SIT_FCT = 'b',
                        COD_TERM_DST = 'c',
                        COD_TIP_EQPM = 'd',
                        IND_TCNL_CMPH = 'e',
                        COD_CNL_DSTR  = 4,
                        COD_CEL_DSTR = 5,
                        DTH_ASSN_CTR_POS = DateTime.Now,
                        VAL_INI_ALGL = 6,
                        COD_INDC_ISN_ALGL = 'f',
                        DTH_FIM_ISN_ALGL = DateTime.Now,
                        COD_INDC_ECLM = 'g',
                        COD_EVN_ESPC = 'h',
                        COD_TIP_LGCO = 7,
                        COD_VDA_DGTD_POS = 'i',
                        COD_INDC_PRRD = 'j',
                        DTH_LIM_PRRD = DateTime.Now,
                        COD_DFR_END = 'k',
                        NOM_END_TERM = 'l',
                        NUM_END_TERM = 'm',
                        NOM_END_CPL_TERM = 'n',
                        NOM_BRR_TERM  = 'o',
                        NOM_CID_TERM  = 'p',
                        NOM_EST_TERM  = 'q',
                        COD_CEP_TERM = 'r',
                        COD_CEP_CPL_TERM = 8,
                        NUM_DDD_TERM = 's',
                        NUM_TEL_TERM = 9,
                        NOM_PES_CTTO = 't',
                        COD_TERM = 'u',
                        NUM_HDSK_ESTB = 10,
                        COD_HRRO_SMNA_INI = 0,
                        COD_HRRO_SMNA_FIM = 0,
                        HOR_INI_FNCN  = 13,
                        HOR_FIM_FNCN = 14,
                        DES_OBS_FCT = "General Kenobi?",
                        COD_FBRC_HDW   = 'v',
                        COD_FORN_SFTW  = 'w',
                        COD_INTG = 'x',
                        COD_TIP_CNXO = 15,
                        COD_VERS_SFTW = 16,
                        NUM_RNPC = 17,
                        COD_PRPT_TERM = 18,
                        QTD_CHCK_OUT = 19,
                        COD_INDC_TEM_PNPD = 'y',
                        COD_OPID_ULT_ATLZ = 'z',
                        NUM_ULT_SEQ_FCT = 20,
                        NUM_ANT = 21,
                        COD_TIP_NMRC = 22,
                        TIMESTAMP = "Hello there.",
                        NUM_FCT_ORI = 23,
                        NUM_PDV_ORI = 24,
                        TIMESTAMP_ORI = "I have the high ground!",
                        COD_INDC_VLDC_ENDR = '$',
                        COD_ACAO_CMC = 25,
                        COD_PRRD_NEGC = 26,
                        IND_ISN_PGMN = '!',
                        COD_CNRO =27,
                        IND_SZND = '?',
                        NUM_FLCO_AMEX = 28,
                        NUM_FLCO_VISA = 29
                    }, null, null, CommandType.StoredProcedure))
                    {
                        while (reader.Read())
                        {
                            var codErr = reader.GetInt32(0);
                            var desErr = reader.GetString(1);
                            var last = reader.GetInt32(2);

                            Assert.AreEqual(0, codErr);
                            Assert.AreEqual("sucesso", desErr);
                            Assert.AreEqual(0, last);
                        }
                    }
                }
            }
        }
    }
}
