using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OLOConteoDiario
{
    public partial class OLOConteoDiario : ServiceBase
    {
        #region "Variables"

        private bool bConfigLoaded = false;         //Indica si la configuración ya fue cargada
        private Microsoft.Win32.RegistryKey regKey; //Entrada del registro de la máquina
        private String sCadenaConexionOLO = "";
        String sFile = "";
        private DateTime fechaEjecucion = DateTime.Today;

        private DataSet dsCabeceraConteoDiario, dsDetalleConteoDiario;
        private DataView dvConteoDiario;

        private String sCompania;
        private static Boolean bVariosClientes;
        private DataSet dsClientes; // lista los clientes cuando bVariosClientes = True
        private String sCliente; //Nombre del cliente en la ejecución actual cuando bVariosClientes = True
        private int iClienteID, iOrganizacionID; // ID de cliente y organisación cuando bVariosClientes = True

        #endregion

        #region "Eventos"

        protected override void OnStart(string[] args)
        {
            StartThreadService();
        }

        protected override void OnStop()
        {
            Variables.WriteLog("Se completó la transacción en " + sCompania);
            Variables.WriteLog("  ");
        }

        #endregion

        #region "Métodos y Funciones"

        private void Ejecutar_Conteo_Diario()
        {
            //validar que sea lunes para hacer la distribucion de muestras
            if (DateTime.Now.DayOfWeek == DayOfWeek.Monday)
            {
                //cargar las poblaciones y la muestra final para el conteo de la semana
                Cargar_Datos_Conteos_Diarios();

                //distribuir los articulos apra el conteo de la semana
                Distribuir_Articulos_Conteo_Diario();
            }
            else 
            {//sino actualiza las paletas para contar en el día actual
                Actualizar_Articulos_Conteo_Diario(iOrganizacionID, iClienteID);
            }
             
        }

        #region "Metodos del servicio"

            public OLOConteoDiario(String pConexion, String _sCompania, Boolean _bVariosClientes)
            {
                InitializeComponent();
                sCadenaConexionOLO = pConexion;
                sCompania = _sCompania;
                bVariosClientes = _bVariosClientes;
                sFile = Variables.sPathBitacora + "\\" + fechaEjecucion.Year.ToString().PadLeft(2, '0') +
                        fechaEjecucion.Month.ToString().PadLeft(2, '0') + fechaEjecucion.Day.ToString().PadLeft(2, '0') + Variables.NOMBRE_BITACORA;
            }

            public void execute()
            {
                StartThreadService();
            
            }

            public void StartThreadService()
            {
                this.IniciarLectura();
            }

            private void IniciarLectura()
            {
                while ((true))
                {
                    try
                    {
                        this.ProcesarRegistros();
                        break;
                    }
                    catch (ThreadInterruptedException)
                    {
                        break;
                    }
                    catch (Exception e)
                    {
                        Variables.WriteLog(" ERROR " + e.ToString());
                        break;
                    }
                }
                this.Stop();
            }

            private void ProcesarRegistros()
            {
                try
                {
                    Variables.WriteLog("Inicio del proceso para : " + sCompania );

                    if (!this.bConfigLoaded)
                    {
                        CargarConfiguracion();
                        if (this.bConfigLoaded)
                        {
                            IniciarFlujo();
                        }
                    }
                    else
                    {
                        IniciarFlujo();
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }

            }

            private void CargarConfiguracion()
        {
            try
            {
                if (!System.IO.Directory.Exists(Variables.sPathBitacora))
                {
                    System.IO.Directory.CreateDirectory(Variables.sPathBitacora);
                }
                Variables.WriteLog("    Cargando configuracion del Servicio.");

                regKey = Microsoft.Win32.Registry.LocalMachine.OpenSubKey(Variables.keyValue, false);

                if ((regKey == null))
                {
                    Variables.WriteLog("        Cargado exitosamente. ");
                    bConfigLoaded = true;
                }
                else
                {
                    Variables.WriteLog("        Servicio aún no ha sido configurado, favor ejecutar el programa de configuración.");
                }
            }
            catch (Exception ex)
            {
                Variables.WriteLog("    Ocurrió el siguiente error al leer la configuración del Servicio: " + ex.Message);
            }
        }

            private void IniciarFlujo()
            {
                try
                {
                    if (bVariosClientes)
                    { //olo5 ZF y NAC
                        //Cargar clientes
                        Cargar_Varios_Clientes();

                        //ejecutar conteo diario por cada cliente registrado
                        Ejecutar_Calculo_Por_Cliente();
                    }
                    else {
                        // Carga datos del cliente 
                        sCliente = sCompania;
                        iClienteID = int.Parse("1000000");
                        iOrganizacionID = int.Parse("1000230");

                    // olo1, olo2 y olo4
                        Ejecutar_Conteo_Diario();

                    }
                }
                catch (Exception ex)
                {
                    Variables.WriteLog("    Error en ciclo de ejecución" + ex);
                }
            }

        #endregion

        #region "Carga de tipos de conteo diario"

            private void Cargar_Datos_Conteos_Diarios()
            {
                dsCabeceraConteoDiario = set_dsCabeceraConteoDiario();
                dsDetalleConteoDiario = set_dsDetalleConteoDiario();

                DateTime dFechaActual = DateTime.Now;
                //FECHA INICIAL
                String sFechaInicial = dFechaActual.AddDays(-8).ToString("yyyy-MM-dd") + " 00:00:00.000000";
                //FECHA FINAL 
                String sFechaFinal = dFechaActual.AddDays(-2).ToString("yyyy-MM-dd") + " 00:00:00.000000";

                if (bVariosClientes)
                {//OLO5 ZF y AN
                    Cargar_Existencias_En_Uno();
                    Cargar_Existencias_Negativas();
                    Cargar_Mayor_Valorizado_Recoleccion(sFechaInicial, sFechaFinal);
                    Cargar_Movimiento_Recoleccion(sFechaInicial, sFechaFinal);
                    Cargar_Mayor_Valorizado_Recibido(sFechaInicial, sFechaFinal);
                    Cargar_Lento_Movimiento();

                    dsDetalleConteoDiario = Cargar_Cantidad_Paletas_General(dsCabeceraConteoDiario, iOrganizacionID, iClienteID);
                }
                else {//OLO1, OLO2 y OLO4
                    Cargar_Existencias_En_Uno();
                    Cargar_Existencias_Negativas();
                    Cargar_Mayor_Valorizado_Recoleccion(sFechaInicial, sFechaFinal);
                    Cargar_Movimiento_Recoleccion(sFechaInicial, sFechaFinal);
                    Cargar_Mayor_Valorizado_Recibido(sFechaInicial, sFechaFinal);
                    Cargar_Lento_Movimiento();

                    Int32 i = Cargar_Ubicaciones_Vacias();

                    dsDetalleConteoDiario = Cargar_Cantidad_Paletas_General(dsCabeceraConteoDiario);
                }

                
            }

            private void Cargar_Existencias_En_Uno ()
            {
                Variables.WriteLog("    Existencias en uno ");
                DataSet dsExistencias;
                Boolean bResultado;
                Int32 iTamanoMuestra = 0, iCantidadPoblacion = 0;

                try
                {
                    LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(this.sCadenaConexionOLO);

                    //carga los articulos que cumplen con la condición
                    if (bVariosClientes)
                    {//OLO5 ZF y AN
                        bResultado = LogicaSQL.Cargar_Existencias_En_Uno(out dsExistencias, iOrganizacionID, iClienteID);
                    }
                    else
                    {//OLO1, OLO2 y OLO4
                        bResultado = LogicaSQL.Cargar_Existencias_En_Uno(out dsExistencias);
                    }

                    //quitar los articulos que se repitan en la cabecera con todos los tipos de conteo diario
                    dsExistencias = Validar_Articulos_Repetidos(dsExistencias);

                    // calcular el tamaño de la muestra
                    iCantidadPoblacion = dsExistencias.Tables[0].Rows.Count;
                    Variables.WriteLog("        Total : " + iCantidadPoblacion + " . ");
                    iTamanoMuestra = Calcular_Muestra(iCantidadPoblacion, "EU");

                    //seleccionar de los datos solo la muestra
                    for (int i = 0; i < iTamanoMuestra; i++)
                    {
                        DataRow row = dsCabeceraConteoDiario.Tables[0].NewRow();
                        row.ItemArray = dsExistencias.Tables[0].Rows[i].ItemArray;
                        dsCabeceraConteoDiario.Tables[0].Rows.Add(row);
                    }

                }
                catch (Exception ex)
                {
                    Variables.WriteLog("        Error al cargar existencias en uno : " + ex);
                }
            }

            private void Cargar_Existencias_Negativas()
            {
                Variables.WriteLog("    Existencias negativas ");
                DataSet dsExistencias;
                Boolean bResultado;
                Int32 iTamanoMuestra = 0, iCantidadPoblacion = 0;

                try
                {
                    LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(this.sCadenaConexionOLO);

                    //carga los articulos que cumplen con la condición
                    if (bVariosClientes)
                    {//OLO5 ZF y AN
                        bResultado = LogicaSQL.Cargar_Existencias_Negativas(out dsExistencias, iOrganizacionID, iClienteID);
                    }
                    else
                    {//OLO1, OLO2 y OLO4
                        bResultado = LogicaSQL.Cargar_Existencias_Negativas(out dsExistencias);
                    }

                    //quitar los articulos que se repitan en la cabecera con todos los tipos de conteo diario
                    dsExistencias = Validar_Articulos_Repetidos(dsExistencias);

                    // calcular el tamaño de la muestra
                    iCantidadPoblacion = dsExistencias.Tables[0].Rows.Count;
                    Variables.WriteLog("        Total : " + iCantidadPoblacion + " . ");
                    iTamanoMuestra = Calcular_Muestra(iCantidadPoblacion, "EN");

                    //seleccionar de los datos solo la muestra
                    for (int i = 0; i < iTamanoMuestra; i++)
                    {
                        DataRow row = dsCabeceraConteoDiario.Tables[0].NewRow();
                        row.ItemArray = dsExistencias.Tables[0].Rows[i].ItemArray;
                        dsCabeceraConteoDiario.Tables[0].Rows.Add(row);
                    }

                }
                catch (Exception ex)
                {
                    Variables.WriteLog("        Error al cargar existencias negativas : " + ex);
                }
            }

            private void Cargar_Mayor_Valorizado_Recoleccion(String sFechaInicial, String sFechaFinal)
            {
                Variables.WriteLog("    Mayor valorizado por recoleccion ");
                DataSet dsExistencias;
                Boolean bResultado;
                Int32 iTamanoMuestra = 0, iCantidadPoblacion = 0;

                try
                {
                    LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(this.sCadenaConexionOLO);

                    //carga los articulos que cumplen con la condición
                    if (bVariosClientes)
                    {//OLO5 ZF y AN
                        bResultado = LogicaSQL.Cargar_Mayor_Valorizado_Recoleccion(sFechaInicial, sFechaFinal, out dsExistencias, iOrganizacionID, iClienteID);
                    }
                    else
                    {//OLO1, OLO2 y OLO4
                        bResultado = LogicaSQL.Cargar_Mayor_Valorizado_Recoleccion(sFechaInicial, sFechaFinal, out dsExistencias);
                    }

                    //quitar los articulos que se repitan en la cabecera con todos los tipos de conteo diario
                    dsExistencias = Validar_Articulos_Repetidos(dsExistencias);

                    // calcular el tamaño de la muestra
                    iCantidadPoblacion = dsExistencias.Tables[0].Rows.Count;
                    Variables.WriteLog("        Total : " + iCantidadPoblacion + " . ");
                    iTamanoMuestra = Calcular_Muestra(iCantidadPoblacion, "VR");

                    //seleccionar de los datos solo la muestra
                    for (int i = 0; i < iTamanoMuestra; i++)
                    {
                        DataRow row = dsCabeceraConteoDiario.Tables[0].NewRow();
                        row.ItemArray = dsExistencias.Tables[0].Rows[i].ItemArray;
                        dsCabeceraConteoDiario.Tables[0].Rows.Add(row);
                    }

                }
                catch (Exception ex)
                {
                    Variables.WriteLog("        Error al cargar mayor valorizado por recoleccion : " + ex);
                }
            }

            private void Cargar_Movimiento_Recoleccion(String sFechaInicial, String sFechaFinal)
            {
                Variables.WriteLog("    Movimientos por recoleccion ");
                DataSet dsExistencias;
                Boolean bResultado;
                Int32 iTamanoMuestra = 0, iCantidadPoblacion = 0;

                try
                {
                    LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(this.sCadenaConexionOLO);

                    //carga los articulos que cumplen con la condición
                    if (bVariosClientes)
                    {//OLO5 ZF y AN
                        bResultado = LogicaSQL.Cargar_Movimiento_Recoleccion(sFechaInicial, sFechaFinal, out dsExistencias, iOrganizacionID, iClienteID);
                    }
                    else
                    {//OLO1, OLO2 y OLO4
                        bResultado = LogicaSQL.Cargar_Movimiento_Recoleccion(sFechaInicial, sFechaFinal, out dsExistencias);
                    }

                    //quitar los articulos que se repitan en la cabecera con todos los tipos de conteo diario
                    dsExistencias = Validar_Articulos_Repetidos(dsExistencias);

                    // calcular el tamaño de la muestra
                    iCantidadPoblacion = dsExistencias.Tables[0].Rows.Count;
                    Variables.WriteLog("        Total : " + iCantidadPoblacion + " . ");
                    iTamanoMuestra = Calcular_Muestra(iCantidadPoblacion, "MR");

                    //seleccionar de los datos solo la muestra
                    for (int i = 0; i < iTamanoMuestra; i++)
                    {
                        DataRow row = dsCabeceraConteoDiario.Tables[0].NewRow();
                        row.ItemArray = dsExistencias.Tables[0].Rows[i].ItemArray;
                        dsCabeceraConteoDiario.Tables[0].Rows.Add(row);
                    }

                }
                catch (Exception ex)
                {
                    Variables.WriteLog("        Error al cargar movimientos por recoleccion : " + ex);
                }
            }

            private void Cargar_Mayor_Valorizado_Recibido(String sFechaInicial, String sFechaFinal)
            {
                Variables.WriteLog("    Mayor valorizado recibido ");
                DataSet dsExistencias;
                Boolean bResultado;
                Int32 iTamanoMuestra = 0, iCantidadPoblacion = 0;

                try
                {
                    LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(this.sCadenaConexionOLO);

                    //carga los articulos que cumplen con la condición
                    if (bVariosClientes)
                    {//OLO5 ZF y AN
                        bResultado = LogicaSQL.Cargar_Mayor_Valorizado_Recibido(sFechaInicial, sFechaFinal, out dsExistencias, iOrganizacionID, iClienteID);
                    }
                    else
                    {//OLO1, OLO2 y OLO4
                        bResultado = LogicaSQL.Cargar_Mayor_Valorizado_Recibido(sFechaInicial, sFechaFinal, out dsExistencias);
                    }

                    //quitar los articulos que se repitan en la cabecera con todos los tipos de conteo diario
                    dsExistencias = Validar_Articulos_Repetidos(dsExistencias);

                    // calcular el tamaño de la muestra
                    iCantidadPoblacion = dsExistencias.Tables[0].Rows.Count;
                    Variables.WriteLog("        Total : " + iCantidadPoblacion + " . ");
                    iTamanoMuestra = Calcular_Muestra(iCantidadPoblacion, "RE");

                    //seleccionar de los datos solo la muestra
                    for (int i = 0; i < iTamanoMuestra; i++)
                    {
                        DataRow row = dsCabeceraConteoDiario.Tables[0].NewRow();
                        row.ItemArray = dsExistencias.Tables[0].Rows[i].ItemArray;
                        dsCabeceraConteoDiario.Tables[0].Rows.Add(row);
                    }

                }
                catch (Exception ex)
                {
                    Variables.WriteLog("        Error al cargar mayor valorizado recibido : " + ex);
                }
            }

            private void Cargar_Lento_Movimiento()
            {
                Variables.WriteLog("    Lento movimiento ");
                DataSet dsExistencias;
                Boolean bResultado;
                Int32 iTamanoMuestra = 0, iCantidadPoblacion = 0;

                try
                {
                    LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(this.sCadenaConexionOLO);

                    //carga los articulos que cumplen con la condición
                    if (bVariosClientes)
                    {//OLO5 ZF y AN
                        bResultado = LogicaSQL.Cargar_Lento_Movimiento(out dsExistencias, iOrganizacionID, iClienteID);
                    }
                    else
                    {//OLO1, OLO2 y OLO4
                        bResultado = LogicaSQL.Cargar_Lento_Movimiento(out dsExistencias);
                    } 

                    //quitar los articulos que se repitan en la cabecera con todos los tipos de conteo diario
                    dsExistencias = Validar_Articulos_Repetidos(dsExistencias);

                    // calcular el tamaño de la muestra
                    iCantidadPoblacion = dsExistencias.Tables[0].Rows.Count;
                    Variables.WriteLog("        Total : " + iCantidadPoblacion + " . ");
                    iTamanoMuestra = Calcular_Muestra(iCantidadPoblacion, "LM");

                    //seleccionar de los datos solo la muestra
                    for (int i = 0; i < iTamanoMuestra; i++)
                    {
                        DataRow row = dsCabeceraConteoDiario.Tables[0].NewRow();
                        row.ItemArray = dsExistencias.Tables[0].Rows[i].ItemArray;
                        dsCabeceraConteoDiario.Tables[0].Rows.Add(row);
                    }

                }
                catch (Exception ex)
                {
                    Variables.WriteLog("        Error al cargar lento movimiento : " + ex);
                }
            }

            private Int32 Cargar_Ubicaciones_Vacias()
        {
            Variables.WriteLog("    Ubicaciones vacias ");
            DataSet dsExistencias;
            Boolean bResultado;
            Int32 iCantidadPoblacion = 0;

            try
            {
                //OLO 2
                LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(Variables.sConnOLO2);

                //carga los articulos que cumplen con la condición
                bResultado = LogicaSQL.Cargar_Ubicaciones_Vacias(out dsExistencias);

                // calcular el tamaño de la muestra
                foreach (DataRow r in dsExistencias.Tables[0].Rows)
                {
                    iCantidadPoblacion = Int32.Parse(r["XX_LO_EXISTENCIA_SISTEMA"].ToString());
                   
                    //inserta los datos en la cabecera
                    if (dsCabeceraConteoDiario != null)
                    {
                        DataRow row = dsCabeceraConteoDiario.Tables[0].NewRow();
                        row.ItemArray = r.ItemArray;
                        dsCabeceraConteoDiario.Tables[0].Rows.Add(row);
                    }
                }

                Variables.WriteLog("        Total : " + iCantidadPoblacion + " . ");

            }
            catch (Exception ex)
            {
                Variables.WriteLog("        Error al cargar ubicaciones vacias : " + ex);
            }

            return iCantidadPoblacion;
        }

            private Int32 Calcular_Muestra(Int32 iPoblacion, String sTipoConteoDiario)
            {
                Int32 iTamanoMuestra = 0;
                Int32 iTamanoMaximo = 0;
                try
                {
                    LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(this.sCadenaConexionOLO);
                    Variables.WriteLog("        Calculo de muestra ... Poblacion: " + iPoblacion + " ");

                    switch (sTipoConteoDiario)
                    {
                        case "EU": //Existencias en uno
                            //validar tope maximo 
                            if (bVariosClientes) //olo5 ZF y AN
                                iTamanoMaximo = 5;
                            else //olo1, olo2 y olo4
                                iTamanoMaximo = 5;

                            //validar si es menor que 5 : todas, sino evalua las primeras 5.
                            if (iPoblacion < iTamanoMaximo)
                                iTamanoMuestra = iPoblacion;
                            else
                                iTamanoMuestra = iTamanoMaximo;

                            break;

                        case "EN": //Existencias negativas
                            //validar tope maximo 
                            if (bVariosClientes) //olo5 ZF y AN
                                iTamanoMaximo = 5;
                            else //olo1, olo2 y olo4
                                iTamanoMaximo = 5;

                            //validar si es menor que 5 : todas, sino evalua las primeras 5.
                            if (iPoblacion < 5)
                                iTamanoMuestra = iPoblacion;
                            else
                                iTamanoMuestra = iTamanoMaximo;

                            break;

                        case "VR": //Mayor valorizado por recolección
                            //validar tope maximo 
                            if (bVariosClientes) //olo5 ZF y AN
                                iTamanoMaximo = LogicaSQL.Calcular_Muestra(iPoblacion, "I", "Normal");
                            else //olo1, olo2 y olo4
                                iTamanoMaximo = LogicaSQL.Calcular_Muestra(iPoblacion, "I", "Normal");

                            if (iPoblacion > 0)
                                iTamanoMuestra = iTamanoMaximo;
                            else
                                iTamanoMuestra = 0;
                            break;

                        case "MR": //Movimientos por recolección

                            //validar tope maximo 
                            if (bVariosClientes) //olo5 ZF y AN
                                iTamanoMaximo = iPoblacion;
                            else //olo1, olo2 y olo4
                                iTamanoMaximo = iPoblacion;


                            if (iPoblacion > 0)
                                iTamanoMuestra = iTamanoMaximo;
                            else
                                iTamanoMuestra = 0;

                            break;

                        case "RE"://Mayor valorizado por recepción
                            //validar tope maximo 
                            if (bVariosClientes) //olo5 ZF y AN
                                iTamanoMaximo = LogicaSQL.Calcular_Muestra(iPoblacion, "I", "Normal");
                            else //olo1, olo2 y olo4
                                iTamanoMaximo = LogicaSQL.Calcular_Muestra(iPoblacion, "I", "Normal");


                            if (iPoblacion > 0)
                                iTamanoMuestra = iTamanoMaximo;
                            break;

                        case "LM"://Lento movimiento
                            //validar tope maximo 
                            if (bVariosClientes) //olo5 ZF y AN
                                iTamanoMaximo = LogicaSQL.Calcular_Muestra(iPoblacion, "II", "Ligera");
                            else //olo1, olo2 y olo4
                                iTamanoMaximo = LogicaSQL.Calcular_Muestra(iPoblacion, "II", "Ligera");

                            if (iPoblacion > 0)
                                iTamanoMuestra = iTamanoMaximo;
                            else
                                iTamanoMuestra = 0;
                            break;

                        case "UV"://Ubicaciones vacías
                            Variables.WriteLog("                todas las ubicaciones vacías");
                            break;
                        default:
                            Variables.WriteLog("                calculo de muestra por defecto");
                            break;
                    }

                    //iTamanoMuestra = LogicaSQL.Calcular_Muestra(iPoblacion, "I", "Normal");

                    Variables.WriteLog("            Tamano de Muestra : " + iTamanoMuestra + " . ");

                }
                catch (Exception ex)
                {
                    Variables.WriteLog("        Error al calcular muestra : " + ex);
                }

                return iTamanoMuestra;
            }

            private DataSet Cargar_Cantidad_Paletas_General(DataSet dsArticulosTipoConteoDiario, int iOrganizacionID = 1000230, int iClienteID = 1000000)
            {
                Variables.WriteLog("    Cargando paletas de artículos ... ");
                DataSet dsPaletasArticulo, dsPaletasTipoConteoDiario;

                dsPaletasTipoConteoDiario = set_dsDetalleConteoDiario();
            
                try
                {
                    LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(this.sCadenaConexionOLO);
                    //Carga las paletas de cada articulo seleccionado para conteo
                    //segun el tipo de conteo
                    foreach (DataRow r in dsArticulosTipoConteoDiario.Tables[0].Rows)
                    {//validar que el contenido sea el mismo que en el actualizar detalle

                        //valida si es todas las paletas o un caso particular 
                        switch (r["TIPO_CONTEO"].ToString())
                        {
                            case "MR": //Movimiento por recoleccion
                                LogicaSQL.Consultar_Paletas_Primer_Nivel(r["M_PRODUCT_ID"].ToString(), out dsPaletasArticulo, iOrganizacionID, iClienteID);
                                break;
                            case "LM": //Lento movimiento
                                LogicaSQL.Consultar_Paletas_Almacen(r["M_PRODUCT_ID"].ToString(), out dsPaletasArticulo, iOrganizacionID, iClienteID);
                                break;
                            case "RE" ://Mayor valorizado recibido
                                LogicaSQL.Consultar_Paletas_SinDespacho(r["M_PRODUCT_ID"].ToString(), out dsPaletasArticulo, iOrganizacionID, iClienteID);
                                break;
                            case "VR"://Mayor valorizado por recoleccion
                                LogicaSQL.Consultar_Paletas_SinDespacho(r["M_PRODUCT_ID"].ToString(), out dsPaletasArticulo, iOrganizacionID, iClienteID);
                                break;
                            default: //EU - EN - UV
                                LogicaSQL.Consultar_Paletas_General(r["M_PRODUCT_ID"].ToString(), out dsPaletasArticulo, iOrganizacionID, iClienteID);
                                break;
                        } 

                        r["CANTIDAD_PALETAS"] = dsPaletasArticulo.Tables[0].Rows.Count;

                        //cargar dsPaletasTipoConteoDiario
                        foreach (DataRow rPaleta in dsPaletasArticulo.Tables[0].Rows) {
                            DataRow row = dsPaletasTipoConteoDiario.Tables[0].NewRow();
                            row.ItemArray = rPaleta.ItemArray;
                            dsPaletasTipoConteoDiario.Tables[0].Rows.Add(row);
                        }
                        Variables.WriteLog("            Artículo : " + r["M_PRODUCT_ID"] + " - cargado correctamente. ");
                    }
                    Variables.WriteLog("            Total de artículos cargados: " + dsCabeceraConteoDiario.Tables[0].Rows.Count + " - paletas cargadas: " + dsPaletasTipoConteoDiario.Tables[0].Rows.Count + ". ");
                }
                catch (Exception ex)
                {
                    Variables.WriteLog("            Error al cargar paletas. " + ex);
                }

                return dsPaletasTipoConteoDiario; 
            }

            private DataSet set_dsCabeceraConteoDiario()
        {
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();
            DataColumn dc;
            dc = new DataColumn("M_PRODUCT_ID", System.Type.GetType("System.String"));
            dt.Columns.Add(dc);

            dc = new DataColumn("XX_LO_REFERENCIA", System.Type.GetType("System.String"));
            dt.Columns.Add(dc);

            dc = new DataColumn("XX_LO_EXISTENCIA_SISTEMA", System.Type.GetType("System.String"));
            dt.Columns.Add(dc);

            dc = new DataColumn("TIPO_CONTEO", System.Type.GetType("System.String"));
            dt.Columns.Add(dc);

            dc = new DataColumn("CANTIDAD_PALETAS", System.Type.GetType("System.Int32"));
            dt.Columns.Add(dc);

            //crea indice en dsCabeceraOrdenCompraOLO4
            dt.PrimaryKey = new DataColumn[] { dt.Columns["M_PRODUCT_ID"]};


            ds.Tables.Add(dt);
            return ds;
        }

            private DataSet set_dsDetalleConteoDiario()
            {
                DataSet ds = new DataSet();
                DataTable dt = new DataTable();
                DataColumn dc;
                dc = new DataColumn("UBICACION", System.Type.GetType("System.String"));
                dt.Columns.Add(dc);

                dc = new DataColumn("PALETA", System.Type.GetType("System.String"));
                dt.Columns.Add(dc);

                dc = new DataColumn("PALETA_ORIGEN", System.Type.GetType("System.String"));
                dt.Columns.Add(dc);

                dc = new DataColumn("CODIGO_OLO", System.Type.GetType("System.String"));
                dt.Columns.Add(dc);

                dc = new DataColumn("M_PRODUCT_ID", System.Type.GetType("System.String"));
                dt.Columns.Add(dc);

                dc = new DataColumn("EXISTENCIA_PALETA", System.Type.GetType("System.String"));
                dt.Columns.Add(dc);

                dc = new DataColumn("EXISTENCIA_TOTAL", System.Type.GetType("System.String"));
                dt.Columns.Add(dc);

                dc = new DataColumn("M_WAREHOUSE_ID", System.Type.GetType("System.String"));
                dt.Columns.Add(dc);

                dc = new DataColumn("M_LOCATOR_ID", System.Type.GetType("System.String"));
                dt.Columns.Add(dc);

                dc = new DataColumn("XX_LO_PURCHASERECORDNO", System.Type.GetType("System.String"));
                dt.Columns.Add(dc);

                dc = new DataColumn("XX_LO_FINALSTATUS", System.Type.GetType("System.String"));
                dt.Columns.Add(dc);
                ds.Tables.Add(dt);

                return ds;
            }

            private DataSet Validar_Articulos_Repetidos(DataSet dsExistencias)
            {
                Variables.WriteLog("        ... validar articulos repetidos para el conteo semanal ... ");
                DataRow drTemp = null;
                Int32 iCantidadRepetidos = 0;
                dsExistencias.Tables[0].PrimaryKey = new DataColumn[] { dsExistencias.Tables[0].Columns["M_PRODUCT_ID"] };

                try
                {
                    foreach (DataRow r in dsCabeceraConteoDiario.Tables[0].Rows)
                    {
                        // Validar que exista 
                        object[] filtro = new object[1];
                        filtro[0] = r["M_PRODUCT_ID"].ToString();

                        // Validar que exista
                        drTemp = dsExistencias.Tables[0].Rows.Find(filtro);

                        if (drTemp != null)
                        {
                            //sacar el articulo de las existencias
                            Variables.WriteLog("            excluyendo el articulo repetido para conteo : " + drTemp["M_PRODUCT_ID"] + " . ");
                            dsExistencias.Tables[0].Rows.Remove(drTemp);
                            iCantidadRepetidos++;
                        }

                        drTemp = null;
                    }
                    Variables.WriteLog("            artículos excluidos : " + iCantidadRepetidos + " ");
                }
                catch (Exception ex)
                {
                    Variables.WriteLog("            error(validar articulos repetidos para el conteo semanal) : " + ex);
                }

                return dsExistencias;
            }

        #endregion

        #region "Actualizar conteo diario  de día "
            private void Actualizar_Articulos_Conteo_Diario(int iOrganizacionID = 1000230, int iClienteID = 1000000)
        {
            Variables.WriteLog("    Actualizando los articulos correspondientes al conteo diario ");
            DataSet dsConteoDiario_Cabecera, dsConteoDiario_Detalle = null;
            Boolean bResultado;
            String  sArticuloID = "", sConteoDiarioID = "", sTipoConteo = "";
            Int32 iCantidadPaletas = 0, iCantidadPaletastmp =0;

            try
            {
                LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(this.sCadenaConexionOLO);

                //quitar detalle de hora
                String sFechaConteoDiario = DateTime.Now.ToString("yyyy-MM-dd") + " 00:00:00.000000";

                //carga los articulos de este conteo diario
                bResultado = LogicaSQL.Cargar_Articulos_Conteo_Diario(sFechaConteoDiario, out dsConteoDiario_Cabecera, iOrganizacionID, iClienteID);

                //por cada artículo de este conteo diario se evalua el tipo y se carga el detalle actualizado
                foreach (DataRow r in dsConteoDiario_Cabecera.Tables[0].Rows)
                {
                    sArticuloID = r["M_PRODUCT_ID"].ToString();
                    sConteoDiarioID = r["XX_LO_CONTEO_DIARIO_ID"].ToString();
                    sTipoConteo = r["TIPO_CONTEO"].ToString();

                    Variables.WriteLog("        M_PRODUCT_ID : " + sArticuloID + " - XX_LO_CONTEO_DIARIO_ID : " + sConteoDiarioID + " - Fecha : " + r["XX_LO_FECHA_CONTEO_DIARIO"].ToString() + " . ");

                    //eliminar el detalle de conteo diario actual
                    bResultado = LogicaSQL.Eliminar_Articulo_Conteo_Diario(sConteoDiarioID, iOrganizacionID, iClienteID);
                    Variables.WriteLog("            eliminando las paletas para el conteo : " + sConteoDiarioID + ". ");

                    //cargar el nuevo detalle de conteo diario - depende del tipo de conteo
                    switch (sTipoConteo)
                    {//validar que el contenido sea el mismo que en el insertar detalle
                        case "MR": //Movimiento por recoleccion
                            LogicaSQL.Consultar_Paletas_Primer_Nivel(r["M_PRODUCT_ID"].ToString(), out dsConteoDiario_Detalle, iOrganizacionID, iClienteID);
                            break;
                        case "LM": //Lento movimiento
                            LogicaSQL.Consultar_Paletas_Almacen(r["M_PRODUCT_ID"].ToString(), out dsConteoDiario_Detalle, iOrganizacionID, iClienteID);
                            break;
                        case "RE"://Mayor valorizado recibido
                            LogicaSQL.Consultar_Paletas_SinDespacho(r["M_PRODUCT_ID"].ToString(), out dsConteoDiario_Detalle, iOrganizacionID, iClienteID);
                            break;
                        case "VR"://Mayor valorizado por recoleccion
                            LogicaSQL.Consultar_Paletas_SinDespacho(r["M_PRODUCT_ID"].ToString(), out dsConteoDiario_Detalle, iOrganizacionID, iClienteID);
                            break;
                        case "UV": //Ubicaciones Vacias
                            dsConteoDiario_Detalle.Clear();
                            //se carga la cantidad de ubicaciones vacias para actualizar
                            iCantidadPaletastmp = Cargar_Ubicaciones_Vacias();
                            break;
                        default: //EU - EN 
                            LogicaSQL.Consultar_Paletas_General(r["M_PRODUCT_ID"].ToString(), out dsConteoDiario_Detalle, iOrganizacionID, iClienteID);
                            break;
                    }

                    //insertar el nuevo detalle de conteo diario
                    foreach (DataRow dr in dsConteoDiario_Detalle.Tables[0].Rows)
                    {
                        LogicaSQL.Insertar_Detalle_Conteo_Diario(sConteoDiarioID, dr["M_WAREHOUSE_ID"].ToString(), dr["M_LOCATOR_ID"].ToString(), dr["PALETA"].ToString(), dr["XX_LO_PURCHASERECORDNO"].ToString()
                                                        , dr["XX_LO_FINALSTATUS"].ToString(), dr["PALETA_ORIGEN"].ToString(), dr["EXISTENCIA_PALETA"].ToString(), iOrganizacionID, iClienteID);
                        
                        Variables.WriteLog("            insertando la paleta : " + dr["PALETA"].ToString() + " - origen : " + dr["PALETA_ORIGEN"].ToString() + " para el conteo : " + sConteoDiarioID + ". ");
                        iCantidadPaletastmp++;
                    }

                    //actualizar la cantidad de paletas para este articulo en la cabecera
                    if(sTipoConteo.Equals("UV"))
                    {
                        //actualizar ubicaciones vacias
                        bResultado = LogicaSQL.Actualizar_Ubicaciones_Vacias_Conteo_Diario(sConteoDiarioID, iCantidadPaletastmp);
                        Variables.WriteLog("            ubicaciones vacias actualizadas : " + iCantidadPaletastmp + " . ");
                    }
                    else{
                        bResultado = LogicaSQL.Actualizar_Articulo_Conteo_Diario(sConteoDiarioID, iOrganizacionID, iClienteID);
                        Variables.WriteLog("            paletas actualizadas : " + iCantidadPaletastmp + " . ");
                        iCantidadPaletastmp = dsConteoDiario_Detalle.Tables[0].Rows.Count;
                        iCantidadPaletas += iCantidadPaletastmp;
                    }

                    iCantidadPaletastmp = 0;
                    dsConteoDiario_Detalle.Clear();
                }

                Variables.WriteLog("        Paletas actualizadas: " + iCantidadPaletas + " . ");

            }
            catch (Exception ex)
            {
                Variables.WriteLog("        Error al actualizar los artículos correspondientes al conteo diario : " + ex);
            }
        }
        #endregion

        #region "Distribuir articulos en la semana"

            private void Distribuir_Articulos_Conteo_Diario()
            {
                Variables.WriteLog("    Distribución de articulos por semana . ");

                //calculo de la cantidad de paletas para cada día
                Int32 iPaletaPorDiaLJ, iPaletaPorDiaV;
                Calcular_Paletas_Diarias(out iPaletaPorDiaLJ, out iPaletaPorDiaV);

                //vista del ds cabecera ordenado por la cantidad de paletas
                DataTable dtConteoDiario = dsCabeceraConteoDiario.Tables[0];
                dvConteoDiario = dtConteoDiario.AsDataView();
                dvConteoDiario.Sort = "CANTIDAD_PALETAS desc";

                //Cargar los datos de la semana
                DataSet dsDistribucionSemanal = set_dsDistribucionSemana();
                dvConteoDiario.Table.PrimaryKey = new DataColumn[] { dvConteoDiario.Table.Columns["M_PRODUCT_ID"] };

                Int32 iDia = 0;
                DateTime dFechaDia = DateTime.Now;
                String sFechaDia = "";

                while (iDia < 5)
                {
                    sFechaDia = dFechaDia.AddDays(iDia).ToString("yyyy-MM-dd") + " 00:00:00.000000";
                    iDia++;

                    switch (iDia)
                    {
                        case 1: //Lunes
                            Variables.WriteLog("        Lunes ");
                            dsDistribucionSemanal = Cargar_Articulo_Dia(iPaletaPorDiaLJ, iDia, sFechaDia, dsDistribucionSemanal, dvConteoDiario);
                            break;
                        case 2: //Martes
                            Variables.WriteLog("        Martes ");
                            dsDistribucionSemanal = Cargar_Articulo_Dia(iPaletaPorDiaLJ, iDia, sFechaDia, dsDistribucionSemanal, dvConteoDiario);
                            break;
                        case 3: //Miercoles
                            Variables.WriteLog("        Miercoles ");
                            dsDistribucionSemanal = Cargar_Articulo_Dia(iPaletaPorDiaLJ, iDia, sFechaDia, dsDistribucionSemanal, dvConteoDiario);
                            break;
                        case 4: //Jueves
                            Variables.WriteLog("        Jueves ");
                            dsDistribucionSemanal = Cargar_Articulo_Dia(iPaletaPorDiaLJ, iDia, sFechaDia, dsDistribucionSemanal, dvConteoDiario);
                            break;
                        case 5: //Viernes
                            Variables.WriteLog("        Viernes ");
                            dsDistribucionSemanal = Cargar_Articulo_Dia(iPaletaPorDiaV, iDia, sFechaDia, dsDistribucionSemanal, dvConteoDiario);
                            break;
                        default:
                            Console.WriteLine("Error en el ciclo para la distribución de conteos por día");
                            break;
                    }

                }

                //insertar conteo diario semanal
                Insertar_Conteo_Diario(dsDistribucionSemanal);
            }

            private void Calcular_Paletas_Diarias(out Int32 iPaletaPorDiaLJ, out Int32 iPaletaPorDiaV)
            {
                iPaletaPorDiaLJ = 0;
                iPaletaPorDiaV = 0;
            
                //calcula la cantidad de paletas que deben contarse por día.

                //Total de paletas en la semana 
                Int32 iTotalPaletas = 0;
                foreach (DataRow r in dsCabeceraConteoDiario.Tables[0].Rows)
                {
                    //excepto las ubicaciones vacias
                    if(!r["TIPO_CONTEO"].ToString().Equals("UV"))
                        iTotalPaletas += Int32.Parse(r["CANTIDAD_PALETAS"].ToString());
                }
            
                Variables.WriteLog("        Total de paletas en la semana : " + iTotalPaletas + " . ");

                //21% ó el máximo - ejemplo : 25.83 = 26
                float tmp = (float) (21 * iTotalPaletas) / 100;
                iPaletaPorDiaLJ = Convert.ToInt32(Math.Ceiling(tmp)); 
                //16%
                iPaletaPorDiaV = (iTotalPaletas - (iPaletaPorDiaLJ * 4));

                Variables.WriteLog("        Total de paletas L-J : " + iPaletaPorDiaLJ + " paletas V : " + iPaletaPorDiaV + " . ");

            }

            private DataSet Cargar_Articulo_Dia(Int32 iPaletasDia, Int32 iDiaActual, String sFechaDia, DataSet dsDistribucionSemana, DataView dvConteoDiario) {
                Int32 iCantidadActual = 0, iCantidadArticulo = 0;
                foreach (DataRowView dr in dvConteoDiario)
                {

                    iCantidadArticulo = Int32.Parse(dr["CANTIDAD_PALETAS"].ToString());

                    //valida el tamaño
                    if ((iCantidadActual+iCantidadArticulo) > iPaletasDia)
                    {
                        //si es el primer articulo y es mayor que iPaletasDia lo inserta para el dia actual y sale
                        if (iCantidadActual == 0)
                        {
                            //lo inserta , lo quita de la vista y sale
                            dsDistribucionSemana = Asignar_Distribucion_Semana(dsDistribucionSemana, iDiaActual, sFechaDia, dr);
                            Variables.WriteLog("            Total de paletas : " + iCantidadArticulo + " ");
                            return dsDistribucionSemana;
                        }
                        else
                        {
                            //si ya alcanzo el iPaletasDia sale
                            if (iCantidadActual == iPaletasDia)
                            {
                                Variables.WriteLog("            Total de paletas : " + iCantidadActual + " ");
                                return dsDistribucionSemana;
                            }
                            //sino lo deja en la vista y sigue en busca de otro
                        }

                    }
                    else {
                        // sino lo inserta, lo quita de la vista y continua buscando...
                        iCantidadActual += iCantidadArticulo;

                        dsDistribucionSemana = Asignar_Distribucion_Semana(dsDistribucionSemana, iDiaActual, sFechaDia, dr);
                   
                    }

                }

                Variables.WriteLog("            Total de paletas : " + iCantidadActual + " ");
                return dsDistribucionSemana;
            }

            private DataSet Asignar_Distribucion_Semana(DataSet dsDistribucionSemana, Int32 iDiaActual, String sFechaDia, DataRowView drView)
            {
                DataRow drConteoDiario;
                DataRow drDistribucionSemana = dsDistribucionSemana.Tables[0].NewRow();
                object[] filtro = new object[1];
                Boolean bValidado = true;
            
                //valida el tipo de conteo, inserta Ubicaciones Vacias solo si es Viernes
                if (drView["TIPO_CONTEO"].ToString().Equals("UV") && iDiaActual != 5)
                    bValidado = false;

                if (bValidado)
                {
                    drDistribucionSemana["DIA"] = iDiaActual;
                    drDistribucionSemana["FECHA"] = sFechaDia;
                    drDistribucionSemana["M_PRODUCT_ID"] = drView["M_PRODUCT_ID"].ToString();
                    drDistribucionSemana["XX_LO_REFERENCIA"] = drView["XX_LO_REFERENCIA"].ToString();
                    drDistribucionSemana["XX_LO_EXISTENCIA_SISTEMA"] = drView["XX_LO_EXISTENCIA_SISTEMA"].ToString();
                    drDistribucionSemana["TIPO_CONTEO"] = drView["TIPO_CONTEO"].ToString();
                    drDistribucionSemana["CANTIDAD_PALETAS"] = drView["CANTIDAD_PALETAS"].ToString();

                    dsDistribucionSemana.Tables[0].Rows.Add(drDistribucionSemana);

                    // Busca la linea para eliminarla
                    filtro[0] = drView["M_PRODUCT_ID"].ToString();
                    drConteoDiario = dvConteoDiario.Table.Rows.Find(filtro);
                    dvConteoDiario.Table.Rows.Remove(drConteoDiario);

                    Variables.WriteLog("            Asignado articulo_id : " + drDistribucionSemana["M_PRODUCT_ID"].ToString()
                                + " - tipo conteo : " + drDistribucionSemana["TIPO_CONTEO"].ToString()
                                + " - cantidad paletas : " + drDistribucionSemana["CANTIDAD_PALETAS"].ToString() 
                                + " . ");
                }

                return dsDistribucionSemana;
            }
        
            private DataSet set_dsDistribucionSemana()
        {
            DataSet ds = new DataSet();
            DataTable dt = new DataTable();
            DataColumn dc;
            dc = new DataColumn("DIA", System.Type.GetType("System.String"));
            dt.Columns.Add(dc);

            dc = new DataColumn("FECHA", System.Type.GetType("System.String"));
            dt.Columns.Add(dc);

            dc = new DataColumn("M_PRODUCT_ID", System.Type.GetType("System.String"));
            dt.Columns.Add(dc);

            dc = new DataColumn("XX_LO_REFERENCIA", System.Type.GetType("System.String"));
            dt.Columns.Add(dc);

            dc = new DataColumn("XX_LO_EXISTENCIA_SISTEMA", System.Type.GetType("System.String"));
            dt.Columns.Add(dc);

            dc = new DataColumn("TIPO_CONTEO", System.Type.GetType("System.String"));
            dt.Columns.Add(dc);

            dc = new DataColumn("CANTIDAD_PALETAS", System.Type.GetType("System.Int32"));
            dt.Columns.Add(dc);

            //crea indice en dsDistribucionSemana
            dt.PrimaryKey = new DataColumn[] { dt.Columns["M_PRODUCT_ID"] };

            ds.Tables.Add(dt);
            return ds;
        }

            private void Insertar_Conteo_Diario(DataSet dsDistribucionSemana)
            {
                Variables.WriteLog("        Insertando conteo diario");
                LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(this.sCadenaConexionOLO);
                Int32 iInsertado = 0, iSinInsertar = 0;
                Int32 iInsertadoDet = 0, iSinInsertarDet = 0, iInsertadotmp = 0, iSinInsertartmp = 0;
                Boolean bResultado = true;

                String sConteoDiarioID = "";

                DataView dvDetalleArticulo;
                DataTable dtDetalleArticulo = dsDetalleConteoDiario.Tables[0];

                try
                {
                    foreach (DataRow r in dsDistribucionSemana.Tables[0].Rows)
                    {
                        //Insertar Cabecera 
                        bResultado = LogicaSQL.Insertar_Cabecera_Conteo_Diario(r["M_PRODUCT_ID"].ToString()
                                                    , r["XX_LO_REFERENCIA"].ToString()
                                                    , r["XX_LO_EXISTENCIA_SISTEMA"].ToString()
                                                    , r["TIPO_CONTEO"].ToString()
                                                    , r["FECHA"].ToString(), out sConteoDiarioID, iOrganizacionID, iClienteID);
                        if (bResultado)
                        {
                            iInsertado++;
                            iInsertadotmp = 0;
                            iSinInsertartmp = 0;

                            Variables.WriteLog("                insertado m_product_id : " + r["M_PRODUCT_ID"].ToString()
                                                               + " - Tipo Conteo : " + r["TIPO_CONTEO"].ToString()
                                                               + " - Día Conteo : " + r["FECHA"].ToString());

                            //filtra paletas solo del articulo a actualizar
                            dvDetalleArticulo = dtDetalleArticulo.AsDataView();

                            dvDetalleArticulo.RowFilter = "M_PRODUCT_ID = '" + r["M_PRODUCT_ID"].ToString() + "'";
                            // dvDetalleArticulo.RowStateFilter = DataViewRowState.ModifiedCurrent;
                            //dvDetalleArticulo.Sort = "M_PRODUCT_ID = " + r["M_PRODUCT_ID"].ToString() + " ";

                            //insertar detalle para ese conteo diario
                            foreach (DataRow dr in dvDetalleArticulo.ToTable().Rows)
                            {
                                bResultado = LogicaSQL.Insertar_Detalle_Conteo_Diario(sConteoDiarioID
                                                            , dr["M_WAREHOUSE_ID"].ToString()
                                                            , dr["M_LOCATOR_ID"].ToString()
                                                            , dr["PALETA"].ToString()
                                                            , dr["XX_LO_PURCHASERECORDNO"].ToString()
                                                            , dr["XX_LO_FINALSTATUS"].ToString()
                                                            , dr["PALETA_ORIGEN"].ToString()
                                                            , dr["EXISTENCIA_PALETA"].ToString(), iOrganizacionID, iClienteID);
                                if (bResultado)
                                    iInsertadotmp++;
                                else
                                {
                                    Variables.WriteLog("                    ! sin insertar m_product_id : " + r["M_PRODUCT_ID"].ToString() + " - paleta : " + dr["PALETA"].ToString() + " ");
                                    iSinInsertartmp++;
                                }
                            }

                            iInsertadoDet += iInsertadotmp;
                            iSinInsertarDet += iSinInsertartmp;
                          
                            Variables.WriteLog("                    detalle insertado : " + iInsertadotmp + " - sin insertar : " + iSinInsertartmp + " ");
                        }
                        else
                        {
                            Variables.WriteLog("                ! sin insertar m_product_id : " + r["M_PRODUCT_ID"].ToString() + " ");
                            iSinInsertar++;
                        }
                    }
                    Variables.WriteLog("            cabecera insertada : " + iInsertado + " - sin insertar : " + iSinInsertar + " ");
                    Variables.WriteLog("            detalle insertado : " + iInsertadoDet + " - sin insertar : " + iSinInsertarDet + " ");
                }
                catch (Exception ex)
                {
                    Variables.WriteLog("            error(insertar cabecera conteo diario) : " + ex);
                }

            }

            
        #endregion

        #region "Ejecución en bVariosClientes = TRUE"
            private void Cargar_Varios_Clientes()
            {
                Boolean bResultado = true;
                dsClientes = null;

                try
                {
                    
                    Variables.WriteLog("    Carga de Clientes de " + sCompania + ". ");

                    LogicaSQL.SQL LogicaSQL = new LogicaSQL.SQL(this.sCadenaConexionOLO);

                    //carga los articulos que cumplen con la condición
                    bResultado = LogicaSQL.Cargar_Clientes_Compania(sCompania, out dsClientes);

                }
                catch (Exception ex)
                {
                    Variables.WriteLog("        Error al cargar listado de clientes de "+sCompania+" : " + ex);
                }


            }

            private void Ejecutar_Calculo_Por_Cliente()
            {
                try
                {
                    foreach (DataRow rCliente in dsClientes.Tables[0].Rows)
                    {
                        // Carga datos del cliente 
                        sCliente = rCliente["CLIENTE"].ToString();
                        iClienteID = int.Parse(rCliente["CLIENTE_ID"].ToString());
                        iOrganizacionID = int.Parse(rCliente["ORGANIZACION_ID"].ToString());

                        // Ejecutar ciclo de conteo diario para este cliente
                        Ejecutar_Conteo_Diario();
                    }

                }
                catch (Exception ex)
                {
                    Variables.WriteLog("        Error al cargar listado de clientes de " + sCompania + " : " + ex);
                }


            }
            
        #endregion

        #endregion
    }
}
