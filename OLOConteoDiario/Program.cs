using AccesoDatos;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;

namespace OLOConteoDiario
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            // ciclo que recorra las compañias y seleccione los articulos y paletas para conteos diarios
            String sCompania= "";
            String pCadenaConexion = "";
            Boolean bMulticompania = false; 
            try
            {
                //1
                for (int i = 1; i < 5; i++)
                {
                    switch (i)
                    {
                        case 1:
                            pCadenaConexion = Variables.sConnOLO1;
                            sCompania = "OLO1";
                            bMulticompania = false;
                            break;
                        case 2:
                            pCadenaConexion = Variables.sConnOLO2;
                            sCompania = "OLO2";
                            bMulticompania = false;
                            break;
                        case 3:
                            pCadenaConexion = Variables.sConnOLO4;
                            sCompania = "OLO4";
                            bMulticompania = false;
                            break;
                        case 4:
                            pCadenaConexion = Variables.sConnOLO5;
                            sCompania = "OLO5";
                            bMulticompania = true;
                            break;
                        default:
                            pCadenaConexion = "";
                            break;
                    }

                    OLOConteoDiario ejecutarServicio = new OLOConteoDiario(pCadenaConexion, sCompania, bMulticompania);
                    ejecutarServicio.execute();
                }

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
