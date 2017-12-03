﻿using System.Data;

namespace AdoNetCore.AseClient
{
    /// <summary>
    /// Represents a parameter to an <see cref="AseCommand" />. This class cannot be inherited.
    /// </summary>
    public sealed class AseDataParameter : IDbDataParameter
    {
        private string _parameterName;

        /// <summary>
        /// Gets or sets the <see cref="DbType" /> of the parameter.
        /// </summary>
        public DbType DbType { get; set; } 

        /// <summary>
        /// Gets or sets a value that indicates whether the parameter is input-only, output-only, 
        /// bidirectional, or a stored procedure return value parameter.
        /// </summary>
        /// <remarks>
        /// <para>If the <see cref="ParameterDirection" /> is output, and execution of the associated <see cref="AseCommand" /> does 
        /// not return a value, the <see cref="AseDataParameter" /> contains a null value.</para>
        /// <para><b>Output</b>, <b>InputOut</b>, and <b>ReturnValue</b> parameters returned by calling <see cref="AseCommand.ExecuteReader" /> cannot 
        /// be accessed until you close the <see cref="AseDataReader" />.</para>
        /// </remarks>
        public ParameterDirection Direction { get; set; }

        /// <summary>
        /// Gets or sets a value that indicates whether the parameter accepts null values. IsNullable is not used to validate the 
        /// parameter’s value and will not prevent sending or receiving a null value when executing a command.
        /// </summary>
        /// <remarks>
        /// <para>Null values are handled using the <see cref="System.DBNull" /> class.</para>
        /// </remarks>
        public bool IsNullable { get; }

        /// <summary>
        /// Gets or sets the name of the <see cref="AseDataParameter" />.
        /// </summary>
        /// <remarks>
        /// <para>The ParameterName is specified in the form @paramname. You must set ParameterName before executing an 
        /// <see cref="AseCommand" /> that relies on parameters.</para>
        /// </remarks>
        public string ParameterName
        {
            get => _parameterName;
            set => _parameterName = value == null || value.StartsWith("@") ? value : $"@{value}";
        }

        /// <summary>
        /// Not supported yet. .NET Core 2.0 dependency.
        /// </summary>
        public string SourceColumn { get; set; }

        /// <summary>
        /// Not supported yet. .NET Core 2.0 dependency.
        /// </summary>
        public DataRowVersion SourceVersion { get; set; }
       
        /// <summary>
        /// Gets or sets the value of the parameter.
        /// </summary>
        /// <remarks>
        /// <para>For input parameters, the value is bound to the <see cref="AseCommand" /> that is sent to the server. For output 
        /// and return value parameters, the value is set on completion of the <see cref="AseCommand" /> and after the <see cref="AseDataReader" /> 
        /// is closed.</para>
        /// <para>This property can be set to null or <see cref="System.DBNull.Value" />. Use <see cref="System.DBNull.Value" /> to send a NULL 
        /// value as the value of the parameter. Use null or do not set Value to use the default value for the parameter.</para>
        /// <para>An exception is thrown if non-Unicode XML data is passed as a string.</para>
        /// <para>If the application specifies the database type, the bound value is converted to that type when the provider sends the 
        /// data to the server. The provider tries to convert any type of value if it supports the <see cref="System.IConvertible" /> 
        /// interface. Conversion errors may result if the specified type is not compatible with the value.</para>
        /// <para>The <see cref="DbType" /> property can be inferred by setting the Value.</para>
        /// </remarks>
        public object Value { get; set; }

        /// <summary>
        /// Gets or sets the number of digits used to represent the <see cref="Value" /> property.
        /// </summary>
        /// <remarks>
        /// <para>The Precision property is used by parameters that have a <see cref="DbType" /> of <see cref="DbType.Decimal" />.</para>
        /// <para>You do not need to specify values for the <see cref="Precision" /> and <see cref="Scale" /> properties for input parameters, as they can be 
        /// inferred from the parameter value. Precision and Scale are required for output parameters and for scenarios where 
        /// you need to specify complete metadata for a parameter without indicating a value, such as specifying a null value 
        /// with a specific precision and scale.</para>
        /// <para>Use of this property to coerce data passed to the database is not supported. To round, truncate, or otherwise coerce data before 
        /// passing it to the database, use the <see cref="System.Math" /> class that is part of the System namespace prior to assigning a value to 
        /// the parameter's <see cref="Value" /> property.</para>
        /// </remarks>
        public byte Precision { get; set; }

        /// <summary>
        /// Gets or sets the number of decimal places to which <see cref="Value" /> is resolved.
        /// </summary>
        /// <remarks>
        /// <para>The Scale property is used by parameters that have a <see cref="DbType" /> of <see cref="DbType.Decimal" />.</para>
        /// <para>Data may be truncated if the Scale property is not explicitly specified and the data on the server does not fit 
        /// in scale 0 (the default).</para>
        /// <para>You do not need to specify values for the <see cref="Precision" /> and <see cref="Scale" /> properties for input parameters, as they can be 
        /// inferred from the parameter value. Precision and Scale are required for output parameters and for scenarios where 
        /// you need to specify complete metadata for a parameter without indicating a value, such as specifying a null value 
        /// with a specific precision and scale.</para>
        /// <para>Use of this property to coerce data passed to the database is not supported. To round, truncate, or otherwise coerce data before 
        /// passing it to the database, use the <see cref="System.Math" /> class that is part of the System namespace prior to assigning a value to 
        /// the parameter's <see cref="Value" /> property.</para>
        /// </remarks>
        public byte Scale { get; set; }

        /// <summary>
        /// Gets or sets the maximum size, in bytes, of the data within the column.
        /// </summary>
        /// <remarks>
        /// <para>Return values are not affected by this property; return parameters from stored procedures are always fixed-size integers.</para>
        /// <para>For output parameters with a variable length type (nvarchar, for example), the size of the parameter defines the size of the 
        /// buffer holding the output parameter. The output parameter can be truncated to a size specified with Size. For character types, the 
        /// size specified with Size is in characters.</para>
        /// <para>The Size property is used for binary and string types. For parameters of type <see cref="DbType.String" />, Size means length in Unicode 
        /// characters. For parameters of type <see cref="DbType.Xml" />, Size is ignored.</para>
        /// <para>For nonstring data types and ANSI string data, the Size property refers to the number of bytes. For Unicode string data, Size refers 
        /// to the number of characters. The count for strings does not include the terminating character.</para>
        /// <para>For variable-length data types, Size describes the maximum amount of data to transmit to the server. For example, for a Unicode 
        /// string value, Size could be used to limit the amount of data sent to the server to the first one hundred characters.</para>
        /// <para>If not explicitly set, the size is inferred from the actual size of the specified parameter value.</para>
        /// <para>If the fractional part of the parameter value is greater than the size, then the value will be truncated to match the size.</para>
        /// <para>For fixed length data types, the value of Size is ignored. It can be retrieved for informational purposes, and returns the 
        /// maximum amount of bytes the provider uses when transmitting the value of the parameter to the server. </para>
        /// </remarks>
        public int Size { get; set; }

        internal bool CanSendOverTheWire => Direction != ParameterDirection.ReturnValue;
        internal bool IsOutput => Direction == ParameterDirection.InputOutput || Direction == ParameterDirection.Output;
    }
}