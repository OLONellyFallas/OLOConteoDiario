using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OLOConteoDiario
{
    class Variables
    {
        //Entrada del Registry
        public static String keyValue = "Desarrollos\\OLOConteoDiario";
        public static string sPathBitacora = "C:\\OLOConteoDiario_Logs";
        public static string NOMBRE_BITACORA = "_bitacora.txt";
        public static Int32 iIntervaloConfigurado = 0;
        public static string sWriteLog = "1";             //Indica si se debe escribir en bitacora: 1 = si, 0 = no
        public static string sDrive = "";                 //letra o drive donde se almacenaran los log y los archivos pdf
        public static string sConnOLO1 = "Provider=IBMDA400.1; Data Source='192.168.118.74'; User ID='COMPIERE'; Password='COMPIERE'; Default Collection='COMPIERE'";
        public static string sConnOLO2 = "Provider=IBMDA400.1; Data Source='192.168.118.75'; User ID='COMPIERE'; Password='COMPIERE'; Default Collection='COMPIERE'";
        public static string sConnOLO4 = "Provider=IBMDA400.1; Data Source='10.48.18.11'; User ID='COMPIERE'; Password='COMPIERE'; Default Collection='COMPIERE'";
        public static string sConnOLO5 = "Provider=IBMDA400.1; Data Source='10.48.18.15'; User ID='COMPIERE'; Password='COMPIERE'; Default Collection='COMPIERE'";

        #region Log

        public static void WriteLog(string strData)
        {
            if (Variables.sWriteLog.CompareTo("1") == 0)
            {
                System.IO.StreamWriter objWriter = null;

                if (!System.IO.Directory.Exists(Variables.sDrive + Variables.sPathBitacora))
                {
                    System.IO.Directory.CreateDirectory(Variables.sDrive + Variables.sPathBitacora);
                }

                string sFile = Variables.sDrive + Variables.sPathBitacora + "\\" + DateTime.Now.Year.ToString().PadLeft(2, '0') + DateTime.Now.Month.ToString().PadLeft(2, '0') + DateTime.Now.Day.ToString().PadLeft(2, '0') + Variables.NOMBRE_BITACORA;
                try
                {
                    if (!System.IO.File.Exists(sFile))
                    {
                        objWriter = System.IO.File.CreateText(sFile);
                    }
                    else
                    {
                        objWriter = new System.IO.StreamWriter(sFile, true);
                    }
                    objWriter.Write(DateTime.Now.ToLongTimeString() + " " + strData);
                    objWriter.WriteLine();
                    objWriter.Close();
                }
                catch (Exception Ex)
                {
                    throw Ex;
                }
            }
        }

        #endregion
    }
}
