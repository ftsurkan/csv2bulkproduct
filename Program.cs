using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace csv2bulkproduct
{
    internal class Program
    {

        const string mssql_tablename = "pi_bulkproduct";

        static void Main(string[] args)
        {

            #region prepare destination

            System.Data.OleDb.OleDbConnection output_connection =
                new System.Data.OleDb.OleDbConnection(System.Configuration.ConfigurationManager.ConnectionStrings["Destination"].ConnectionString);
            output_connection.Open();

            System.Data.OleDb.OleDbCommand mssql_command = new System.Data.OleDb.OleDbCommand(
                $"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{mssql_tablename}' ", output_connection);

            if (!mssql_command.ExecuteReader().Read())
            {
                mssql_command = new System.Data.OleDb.OleDbCommand(create_table_sql, output_connection);
                mssql_command.ExecuteNonQuery();
            }
            else
            {
                mssql_command = new System.Data.OleDb.OleDbCommand($"DELETE FROM [{mssql_tablename}]", output_connection);
                mssql_command.ExecuteNonQuery();
            }

            mssql_command = new System.Data.OleDb.OleDbCommand(
                insert_table_sql, output_connection);


            System.Data.OleDb.OleDbParameter prmCategory     = mssql_command.Parameters.Add("@Category", System.Data.OleDb.OleDbType.VarWChar, 10  , "Category" );
            System.Data.OleDb.OleDbParameter prmDesc         = mssql_command.Parameters.Add("@Desc", System.Data.OleDb.OleDbType.VarWChar, 35      , "Desc" );
            System.Data.OleDb.OleDbParameter prmCode         = mssql_command.Parameters.Add("@Code", System.Data.OleDb.OleDbType.VarWChar, 50      , "Code" );
            System.Data.OleDb.OleDbParameter prmBrand        = mssql_command.Parameters.Add("@Brand", System.Data.OleDb.OleDbType.VarWChar, 15     , "Brand" );
            System.Data.OleDb.OleDbParameter prmSize         = mssql_command.Parameters.Add("@Size", System.Data.OleDb.OleDbType.VarWChar, 6       , "Size" );
            System.Data.OleDb.OleDbParameter prmUPC          = mssql_command.Parameters.Add("@UPC", System.Data.OleDb.OleDbType.BigInt, 0          , "UPC" );
            System.Data.OleDb.OleDbParameter prmLast_Sold_On = mssql_command.Parameters.Add("@Last_Sold_On", System.Data.OleDb.OleDbType.Date, 0   , "Last_Sold_On" );
            System.Data.OleDb.OleDbParameter prmPrice        = mssql_command.Parameters.Add("@Price", System.Data.OleDb.OleDbType.Currency, 0      , "Price" );
            System.Data.OleDb.OleDbParameter prmSale         = mssql_command.Parameters.Add("@Sale", System.Data.OleDb.OleDbType.Currency, 0       , "Sale" );
            System.Data.OleDb.OleDbParameter prmBegins       = mssql_command.Parameters.Add("@Begins", System.Data.OleDb.OleDbType.Date, 0         , "Begins" );
            System.Data.OleDb.OleDbParameter prmExpires      = mssql_command.Parameters.Add("@Expires", System.Data.OleDb.OleDbType.Date, 0        , "Expires" );
            System.Data.OleDb.OleDbParameter prmMax          = mssql_command.Parameters.Add("@Max", System.Data.OleDb.OleDbType.BigInt, 0          , "Max" );
            System.Data.OleDb.OleDbParameter prmCasePrice    = mssql_command.Parameters.Add("@CasePrice", System.Data.OleDb.OleDbType.Currency, 0  , "CasePrice" );
            System.Data.OleDb.OleDbParameter prmCaseSale     = mssql_command.Parameters.Add("@CaseSale", System.Data.OleDb.OleDbType.Currency, 0   , "CaseSale" );
            System.Data.OleDb.OleDbParameter prmCBegins      = mssql_command.Parameters.Add("@CBegins", System.Data.OleDb.OleDbType.Date, 0        , "CBegins" );
            System.Data.OleDb.OleDbParameter prmCExpires     = mssql_command.Parameters.Add("@CExpires", System.Data.OleDb.OleDbType.Date, 0       , "CExpires" );
            System.Data.OleDb.OleDbParameter prmCMax         = mssql_command.Parameters.Add("@CMax", System.Data.OleDb.OleDbType.BigInt, 0         , "CMax" );
            System.Data.OleDb.OleDbParameter prmUpp          = mssql_command.Parameters.Add("@Upp", System.Data.OleDb.OleDbType.BigInt, 0          , "Upp" );
            System.Data.OleDb.OleDbParameter prmPkgPrc       = mssql_command.Parameters.Add("@PkgPrc", System.Data.OleDb.OleDbType.Currency, 0     , "PkgPrc" );
            System.Data.OleDb.OleDbParameter prmSUpp         = mssql_command.Parameters.Add("@SUpp", System.Data.OleDb.OleDbType.BigInt, 0         , "SUpp" );
            System.Data.OleDb.OleDbParameter prmDepartment   = mssql_command.Parameters.Add("@Department", System.Data.OleDb.OleDbType.VarWChar, 6 , "Department"  );
            System.Data.OleDb.OleDbParameter prmSPkgPrc      = mssql_command.Parameters.Add("@SPkgPrc", System.Data.OleDb.OleDbType.Currency, 0    , "SPkgPrc" );
            System.Data.OleDb.OleDbParameter prmPBegins      = mssql_command.Parameters.Add("@PBegins", System.Data.OleDb.OleDbType.Date, 0        , "PBegins" );
            System.Data.OleDb.OleDbParameter prmPExpires     = mssql_command.Parameters.Add("@PExpires", System.Data.OleDb.OleDbType.Date, 0       , "PExpires" );
            System.Data.OleDb.OleDbParameter prmPMax         = mssql_command.Parameters.Add("@PMax", System.Data.OleDb.OleDbType.BigInt, 0         , "PMax" );
            System.Data.OleDb.OleDbParameter prmMeasure      = mssql_command.Parameters.Add("@Measure", System.Data.OleDb.OleDbType.VarWChar, 5    , "Measure" );
            System.Data.OleDb.OleDbParameter prmUnits        = mssql_command.Parameters.Add("@Units", System.Data.OleDb.OleDbType.VarWChar, 9      , "Units" );
            System.Data.OleDb.OleDbParameter prmFS           = mssql_command.Parameters.Add("@FS", System.Data.OleDb.OleDbType.Boolean, 0          , "FS" );
            System.Data.OleDb.OleDbParameter prmTax          = mssql_command.Parameters.Add("@Tax", System.Data.OleDb.OleDbType.Boolean, 0         , "Tax" );
            System.Data.OleDb.OleDbParameter prmWic          = mssql_command.Parameters.Add("@Wic", System.Data.OleDb.OleDbType.Boolean, 0         , "Wic" );
            System.Data.OleDb.OleDbParameter prmOnHand       = mssql_command.Parameters.Add("@OnHand", System.Data.OleDb.OleDbType.Decimal,0       , "OnHand" );
            prmOnHand.Precision = 15; prmOnHand.Scale = 2; 
            System.Data.OleDb.OleDbParameter prmSeason       = mssql_command.Parameters.Add("@Season", System.Data.OleDb.OleDbType.VarWChar, 4     , "Season" );
            System.Data.OleDb.OleDbParameter prmPrdRule      = mssql_command.Parameters.Add("@PrdRule", System.Data.OleDb.OleDbType.VarWChar, 8    , "PrdRule" );
            System.Data.OleDb.OleDbParameter prmNewField     = mssql_command.Parameters.Add("@NewField", System.Data.OleDb.OleDbType.BigInt, 0     , "NewField" );


            mssql_command.Prepare();

            #endregion



            #region prepare source

            System.Data.OleDb.OleDbConnection input_connection =
                new System.Data.OleDb.OleDbConnection(System.Configuration.ConfigurationManager.ConnectionStrings["Source"].ConnectionString);

            input_connection.Open();

            System.Data.OleDb.OleDbCommand sel_command = new System.Data.OleDb.OleDbCommand(
                " SELECT * FROM [IMPORT_DATA2.CSV] ", input_connection);

            System.Data.OleDb.OleDbDataReader reader = sel_command.ExecuteReader();

            #endregion


            while (reader.Read())
            {
                prmCategory          .Value = reader["Category"];
                prmDesc              .Value = reader["Desc"];
                prmCode              .Value = reader["Code"];
                prmBrand             .Value = reader["Brand"];
                prmSize              .Value = reader["Size"];
                prmUPC               .Value = reader["UPC"];
                prmLast_Sold_On      .Value = reader["Last Sold On"];
                prmPrice             .Value = reader["Price"];
                prmSale              .Value = reader["Sale"];
                prmBegins            .Value = reader["Begins"];
                prmExpires           .Value = reader["Expires"];
                prmMax               .Value = reader["Max"];
                prmCasePrice         .Value = reader["CasePrice"];
                prmCaseSale          .Value = reader["CaseSale"];
                prmCBegins           .Value = reader["CBegins"];
                prmCExpires          .Value = reader["CExpires"];
                prmCMax              .Value = reader["CMax"];
                prmUpp               .Value = reader["Upp"];
                prmPkgPrc            .Value = reader["PkgPrc"];
                prmSUpp              .Value = reader["SUpp"];
                prmDepartment        .Value = reader["Department"];
                prmSPkgPrc           .Value = reader["SPkgPrc"];
                prmPBegins           .Value = reader["PBegins"];
                prmPExpires          .Value = reader["PExpires"];
                prmPMax              .Value = reader["PMax"];
                prmMeasure           .Value = reader["Measure"];
                prmUnits             .Value = reader["Units"];
                prmFS                .Value = reader["FS"];
                prmTax               .Value = reader["Tax"];
                prmWic               .Value = reader["Wic"];
                
                prmOnHand            .Value = processOnHandColumn( Convert.ToString( reader["OnHand"] ) ) ;
                
                prmSeason            .Value = reader["Season"];
                prmPrdRule           .Value = reader["PrdRule"];
                prmNewField          .Value = reader["NewField"];


                mssql_command.ExecuteNonQuery();

            }
            Console.WriteLine("Total");
        }


#region long sql operators

        const string create_table_sql =
            "CREATE TABLE [dbo].[" + mssql_tablename + "](    " +
            "    [Id][int] IDENTITY(1,1) NOT NULL," + //
            "    [Category][nvarchar](10) NULL,         " +
            "    [Desc][nvarchar] (35) NULL,            " +
	        "    [Code][nvarchar] (50) NULL,            " +
	        "    [Brand][nvarchar] (15) NULL,           " +
	        "    [Size][nvarchar] (6) NULL,             " +
	        "    [UPC][bigint] NULL,                    " +
	        "    [Last Sold On][datetime] NULL,         " +
	        "    [Price][money] NULL,                   " +
	        "    [Sale][money] NULL,                    " +
	        "    [Begins][datetime] NULL,               " +
	        "    [Expires][datetime] NULL,              " +
	        "    [Max][bigint] NULL,                    " +
	        "    [CasePrice][money] NULL,               " +
	        "    [CaseSale][money] NULL,                " +
	        "    [CBegins][datetime] NULL,              " +
	        "    [CExpires][datetime] NULL,             " +
	        "    [CMax][bigint] NULL,                   " +
	        "    [Upp][bigint] NULL,                    " +
	        "    [PkgPrc][money] NULL,                  " +
	        "    [SUpp][bigint] NULL,                   " +
	        "    [Department][nvarchar] (6) NULL,       " +
	        "    [SPkgPrc][money] NULL,                 " +
	        "    [PBegins][datetime] NULL,              " +
	        "    [PExpires][datetime] NULL,             " +
	        "    [PMax][bigint] NULL,                   " +
	        "    [Measure][nvarchar] (5) NULL,          " +
	        "    [Units][nvarchar] (9) NULL,            " +
	        "    [FS][bit] NULL,                        " +
	        "    [Tax][bit] NULL,                       " +
	        "    [Wic][bit] NULL,                       " +
            "    [OnHand][decimal](15,2) NULL,          " +
	        "    [Season][nvarchar] (4) NULL,           " +
	        "    [PrdRule][nvarchar] (8) NULL,          " +
	        "    [NewField][bigint] NULL                " +
            ") ON [PRIMARY] " ;


        const string insert_table_sql =
           " INSERT INTO [dbo].[" + mssql_tablename + "] ( " +
           " [Category]        " +
           ",[Desc]            " +
           ",[Code]            " +
           ",[Brand]           " +
           ",[Size]            " +
           ",[UPC]             " +
           ",[Last Sold On]    " +
           ",[Price]           " +
           ",[Sale]            " +
           ",[Begins]          " +
           ",[Expires]         " +
           ",[Max]             " +
           ",[CasePrice]       " +
           ",[CaseSale]        " +
           ",[CBegins]         " +
           ",[CExpires]        " +
           ",[CMax]            " +
           ",[Upp]             " +
           ",[PkgPrc]          " +
           ",[SUpp]            " +
           ",[Department]      " +
           ",[SPkgPrc]         " +
           ",[PBegins]         " +
           ",[PExpires]        " +
           ",[PMax]            " +
           ",[Measure]         " +
           ",[Units]           " +
           ",[FS]              " +
           ",[Tax]             " +
           ",[Wic]             " +
           ",[OnHand]          " +
           ",[Season]          " +
           ",[PrdRule]         " +
           ",[NewField]        " +
           " ) VALUES (        " +
           "  ?   " + //@Category     
           ", ?   " + //@Desc         
           ", ?   " + //@Code         
           ", ?   " + //@Brand        
           ", ?   " + //@Size         
           ", ?   " + //@UPC          
           ", ?   " + //@Last_Sold_On 
           ", ?   " + //@Price        
           ", ?   " + //@Sale         
           ", ?   " + //@Begins       
           ", ?   " + //@Expires      
           ", ?   " + //@Max          
           ", ?   " + //@CasePrice    
           ", ?   " + //@CaseSale     
           ", ?   " + //@CBegins      
           ", ?   " + //@CExpires     
           ", ?   " + //@CMax         
           ", ?   " + //@Upp          
           ", ?   " + //@PkgPrc       
           ", ?   " + //@SUpp         
           ", ?   " + //@Department   
           ", ?   " + //@SPkgPrc      
           ", ?   " + //@PBegins      
           ", ?   " + //@PExpires     
           ", ?   " + //@PMax         
           ", ?   " + //@Measure      
           ", ?   " + //@Units        
           ", ?   " + //@FS           
           ", ?   " + //@Tax          
           ", ?   " + //@Wic          
           ", ?   " + //@OnHand       
           ", ?   " + //@Season       
           ", ?   " + //@PrdRule      
           ", ?   " + //@NewField     
           ")";

        #endregion


        static Object processOnHandColumn(String _OnHandValue )
        {

            if (String.IsNullOrEmpty(_OnHandValue))
                return DBNull.Value;

            if (_OnHandValue.StartsWith("("))
            {
                _OnHandValue = _OnHandValue.Trim(new char[] { '(', ')' });

                return -Decimal.Parse(_OnHandValue, System.Globalization.CultureInfo.GetCultureInfo(1033));
            }
            else
            {
                return Decimal.Parse(_OnHandValue, System.Globalization.CultureInfo.GetCultureInfo(1033));
            }

        }

    }
}
