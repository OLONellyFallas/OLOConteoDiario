using AccesoDatos;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LogicaSQL
{
    public class SQL
    {
        // agregando cambios para probar commit

        #region Variables

        private String sCadenaConexion;                 //cadena para el acceso a datos
        private AccesoDatos.clsConexionOleDB Conexion; //Instancia para el acceso a datos

        #endregion

        #region Propiedades

        /// <summary>
        /// Propiedad para la cadena de conexion
        /// </summary>
        public String CadenaConexion
        {
            get { return sCadenaConexion; }
            set { sCadenaConexion = value; }
        }

        #endregion

        #region Constructores

        /// <summary>
        /// Constructor de la clase
        /// </summary>
        /// <param name="pCadenaConexion">cadena para realizar la conexion</param>
        public SQL(String pCadenaConexion)
        {
            this.sCadenaConexion = pCadenaConexion;
        }

        #endregion

        #region Metodos y Funciones

            #region Consultas
            
                /// <summary>
                /// carga datos - Existencias en uno
                /// </summary>
                /// <returns></returns>
                public Boolean Cargar_Tipos_Conteos_Diarios(out DataSet dsTiposConteosDiarios)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {

                        CmdSelect.CommandText =
                               "SELECT L.VALUE AS TIPO_CONTEO , L.NAME AS DESCRIPCION FROM AD_REF_LIST L " +
                                "	INNER JOIN AD_REFERENCE R ON L.AD_REFERENCE_ID = R.AD_REFERENCE_ID " +
                                "		AND R.NAME = 'XX_LO_CONTEO_DIARIO_TIPO' ";

                        dsTiposConteosDiarios = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        bResultado = false;
                        throw e;
                    }
                    return bResultado;
                }

                /// <summary>
                /// Carga las paletas para un artículo en particulas.
                /// </summary>
                /// <returns></returns>
                public void Consultar_Paletas_General(String sArticuloID, out DataSet dsPaletas, int iOrganizacionID, int iClienteID)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();

                    try
                    {

                        CmdSelect.CommandText =
                               "SELECT *  FROM ( " +
                                "   SELECT LOC.VALUE UBICACION " +
                                "       , PRP.XX_LO_PALLET_LICENSE PALETA " +
                                "       , PRP.XX_LO_PALLET_LICENSE  AS PALETA_ORIGEN " +
                                "       , MPO.VALUE AS CODIGO_OLO " +
                                "        , MPO.M_PRODUCT_ID AS M_PRODUCT_ID " +
                                "       , PRP.XX_LO_PALLET_QTY_EXISTS AS EXISTENCIA_PALETA " +
                                "       , MS.QTYONHAND AS EXISTENCIA_TOTAL " +
                                "       , PRP.M_WAREHOUSE_ID " +
                                "       , LOC.M_LOCATOR_ID " +
                                "       , PRP.XX_LO_PURCHASERECORDNO AS XX_LO_PURCHASERECORDNO	" +
                                "		, PR.XX_LO_FINALSTATUS AS XX_LO_FINALSTATUS	" +
                                "   FROM XX_LO_PURCHASERECORD_PALLET PRP " +
                                "       INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO) " +
                                "       INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PRP.M_LOCATOR_ID ) " +
                                "       INNER JOIN M_PRODUCT        MPO ON (MPO.M_PRODUCT_ID = PRP.M_PRODUCT_ID) " +
                                "              INNER JOIN M_STORAGE	MS ON (MS.M_PRODUCT_ID = MPO.M_PRODUCT_ID ) " +
                                "   WHERE PRP.M_PRODUCT_ID IN (SELECT M_PRODUCT_ID  FROM M_PRODUCT_PO WHERE MPO.M_PRODUCT_ID IN ( " + sArticuloID + " )) " +
                                "       AND MPO.AD_CLIENT_ID = " + iClienteID + " " + //AND MPO.AD_ORG_ID = " + iOrganizacionID + " " +
                                "       AND PRP.XX_LO_PALLET_QTY_EXISTS > 0 " +
                                "       AND PRP.XX_LO_INVOICED = 'N' " +
                                "       AND LOC.VALUE <> 'V00-000-00-00-00' " +
                                "       AND PR.XX_LO_ISDROPSHIP        = 'N' " +
                                "       AND PRP.XX_LO_PALLET_QTY_EXCESS = 0 " +
                                "   UNION ALL " +
                                "   SELECT LOC.VALUE AS UBICACION " +
                                "       , PPA.XX_LO_PROCESS_PALLET_LICENSE AS PALETA " +
                                "       , PPL.XX_LO_PURCHASERECORD_PALLET_LICENSE AS PALETA_ORIGEN " +
                                "       , MPO.VALUE AS CODIGO_OLO " +
                                "       , MPO.M_PRODUCT_ID " +
                                "       , PPL.XX_LO_ONHAND AS EXISTENCIA_PALETA " +
                                "       , MS.QTYONHAND AS EXISTENCIA_TOTAL " +
                                "       , PPA.M_WAREHOUSE_ID " +
                                "       , LOC.M_LOCATOR_ID " +
                                "       , PPL.XX_LO_PURCHASERECORDNO AS XX_LO_PURCHASERECORDNO	" +
                                "		, PR.XX_LO_FINALSTATUS AS XX_LO_FINALSTATUS	" +
                                "   FROM XX_LO_PROCESS_PALLET_LINE PPL " +
                                "       INNER JOIN XX_LO_PROCESS_PALLET        PPA ON (PPA.XX_LO_PROCESS_PALLET_ID = PPL.XX_LO_PROCESS_PALLET_ID) " +
                                "       LEFT JOIN (XX_LO_PURCHASERECORD_PALLET PRP " +
                                "               INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO)    ) " +
                                "           ON (PRP.XX_LO_PALLET_LICENSE = PPL.XX_LO_PURCHASERECORD_PALLET_LICENSE AND PRP.M_PRODUCT_ID = PPL.M_PRODUCT_ID) " +
                                "       INNER JOIN M_LOCATOR                   LOC ON (LOC.M_LOCATOR_ID = PPA.M_LOCATOR_ID ) " +
                                "       INNER JOIN M_PRODUCT MPO ON (MPO.M_PRODUCT_ID = PPL.M_PRODUCT_ID) " +
                                "              INNER JOIN M_STORAGE	MS ON (MS.M_PRODUCT_ID = MPO.M_PRODUCT_ID ) " +
                                "   WHERE PPL.M_PRODUCT_ID IN (SELECT M_PRODUCT_ID FROM M_PRODUCT_PO WHERE MPO.M_PRODUCT_ID IN (" + sArticuloID + ")) " +
                                "       AND MPO.AD_CLIENT_ID = " + iClienteID + " " + //AND MPO.AD_ORG_ID = " + iOrganizacionID + " " +
                                "       AND PPL.XX_LO_ONHAND > 0 " +
                                "       AND PPA.XX_LO_INVOICED = 'N' " +
                                "       AND LOC.VALUE <> 'V00-000-00-00-00' " +
                                ") AS RESULT_UBICA " +
                                "ORDER BY UBICACION ASC ";

                        dsPaletas = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        throw e;
                    }
                }

                /// <summary>
                /// Carga las paletas para un artículo - excluye ubicaciones de despacho.
                /// </summary>
                /// <returns></returns>
                public void Consultar_Paletas_SinDespacho(String sArticuloID, out DataSet dsPaletas, int iOrganizacionID, int iClienteID)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();

                    try
                    {

                        CmdSelect.CommandText =
                               "SELECT *  FROM ( " +
                                "   SELECT LOC.VALUE UBICACION " +
                                "       , PRP.XX_LO_PALLET_LICENSE PALETA " +
                                "       , PRP.XX_LO_PALLET_LICENSE  AS PALETA_ORIGEN " +
                                "       , MPO.VALUE AS CODIGO_OLO " +
                                "        , MPO.M_PRODUCT_ID AS M_PRODUCT_ID " +
                                "       , PRP.XX_LO_PALLET_QTY_EXISTS AS EXISTENCIA_PALETA " +
                                "       , MS.QTYONHAND AS EXISTENCIA_TOTAL " +
                                "       , PRP.M_WAREHOUSE_ID " +
                                "       , LOC.M_LOCATOR_ID " +
                                "       , PRP.XX_LO_PURCHASERECORDNO AS XX_LO_PURCHASERECORDNO	" +
                                "		, PR.XX_LO_FINALSTATUS AS XX_LO_FINALSTATUS	" +
                                "   FROM XX_LO_PURCHASERECORD_PALLET PRP " +
                                "       INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO) " +
                                "       INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PRP.M_LOCATOR_ID ) AND SUBSTR(LOC.VALUE, 0, 2) NOT IN ('X', 'D')  " +
                                "       INNER JOIN M_PRODUCT        MPO ON (MPO.M_PRODUCT_ID = PRP.M_PRODUCT_ID) " +
                                "              INNER JOIN M_STORAGE	MS ON (MS.M_PRODUCT_ID = MPO.M_PRODUCT_ID ) " +
                                "   WHERE PRP.M_PRODUCT_ID IN (SELECT M_PRODUCT_ID  FROM M_PRODUCT_PO WHERE MPO.M_PRODUCT_ID IN ( " + sArticuloID + " )) " +
                                "       AND MPO.AD_CLIENT_ID = " + iClienteID + " " + //AND MPO.AD_ORG_ID = " + iOrganizacionID + " " +
                                "       AND PRP.XX_LO_PALLET_QTY_EXISTS > 0 " +
                                "       AND PRP.XX_LO_INVOICED = 'N' " +
                                "       AND LOC.VALUE <> 'V00-000-00-00-00' " +
                                "       AND PR.XX_LO_ISDROPSHIP        = 'N' " +
                                "       AND PRP.XX_LO_PALLET_QTY_EXCESS = 0 " +
                                "   UNION ALL " +
                                "   SELECT LOC.VALUE AS UBICACION " +
                                "       , PPA.XX_LO_PROCESS_PALLET_LICENSE AS PALETA " +
                                "       , PPL.XX_LO_PURCHASERECORD_PALLET_LICENSE AS PALETA_ORIGEN " +
                                "       , MPO.VALUE AS CODIGO_OLO " +
                                "       , MPO.M_PRODUCT_ID " +
                                "       , PPL.XX_LO_ONHAND AS EXISTENCIA_PALETA " +
                                "       , MS.QTYONHAND AS EXISTENCIA_TOTAL " +
                                "       , PPA.M_WAREHOUSE_ID " +
                                "       , LOC.M_LOCATOR_ID " +
                                "       , PPL.XX_LO_PURCHASERECORDNO AS XX_LO_PURCHASERECORDNO	" +
                                "		, PR.XX_LO_FINALSTATUS AS XX_LO_FINALSTATUS	" +
                                "   FROM XX_LO_PROCESS_PALLET_LINE PPL " +
                                "       INNER JOIN XX_LO_PROCESS_PALLET        PPA ON (PPA.XX_LO_PROCESS_PALLET_ID = PPL.XX_LO_PROCESS_PALLET_ID) " +
                                "       LEFT JOIN (XX_LO_PURCHASERECORD_PALLET PRP " +
                                "               INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO)    ) " +
                                "           ON (PRP.XX_LO_PALLET_LICENSE = PPL.XX_LO_PURCHASERECORD_PALLET_LICENSE AND PRP.M_PRODUCT_ID = PPL.M_PRODUCT_ID) " +
                                "       INNER JOIN M_LOCATOR                   LOC ON (LOC.M_LOCATOR_ID = PPA.M_LOCATOR_ID ) AND SUBSTR(LOC.VALUE, 0, 2) NOT IN ('X', 'D')  " +
                                "       INNER JOIN M_PRODUCT MPO ON (MPO.M_PRODUCT_ID = PPL.M_PRODUCT_ID) " +
                                "              INNER JOIN M_STORAGE	MS ON (MS.M_PRODUCT_ID = MPO.M_PRODUCT_ID ) " +
                                "   WHERE PPL.M_PRODUCT_ID IN (SELECT M_PRODUCT_ID FROM M_PRODUCT_PO WHERE MPO.M_PRODUCT_ID IN (" + sArticuloID + ")) " +
                                "       AND MPO.AD_CLIENT_ID = " + iClienteID + " " + //AND MPO.AD_ORG_ID = " + iOrganizacionID + " " +
                                "       AND PPL.XX_LO_ONHAND > 0 " +
                                "       AND PPA.XX_LO_INVOICED = 'N' " +
                                "       AND LOC.VALUE <> 'V00-000-00-00-00' " +
                                ") AS RESULT_UBICA " +
                                "ORDER BY UBICACION ASC ";

                        dsPaletas = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        throw e;
                    }
                }

                /// <summary>
                /// Carga las paletas para un artículo en particular.
                /// Que se encuentre en primer nivel.
                /// </summary>
                /// <returns></returns>
                public void Consultar_Paletas_Primer_Nivel(String sArticuloID, out DataSet dsPaletas, int iOrganizacionID, int iClienteID)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();

                    try
                    {

                        CmdSelect.CommandText =
                               "SELECT *  FROM ( " +
                                "   SELECT LOC.VALUE UBICACION " +
                                "       , PRP.XX_LO_PALLET_LICENSE PALETA " +
                                "       , PRP.XX_LO_PALLET_LICENSE  AS PALETA_ORIGEN " +
                                "       , MPO.VALUE AS CODIGO_OLO " +
                                "       , MPO.M_PRODUCT_ID AS M_PRODUCT_ID " +
                                "       , PRP.XX_LO_PALLET_QTY_EXISTS AS EXISTENCIA_PALETA " +
                                "       , MS.QTYONHAND AS EXISTENCIA_TOTAL " +
                                "       , PRP.M_WAREHOUSE_ID " +
                                "       , LOC.M_LOCATOR_ID " +
                                "       , PRP.XX_LO_PURCHASERECORDNO AS XX_LO_PURCHASERECORDNO	" +
                                "		, PR.XX_LO_FINALSTATUS AS XX_LO_FINALSTATUS	" +
                                "   FROM XX_LO_PURCHASERECORD_PALLET PRP " +
                                "       INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO) " +
                                "       INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PRP.M_LOCATOR_ID AND SUBSTR(LOC.VALUE, 0, 2) IN ('0') AND SUBSTR(LOC.VALUE, 9, 2) IN ('01') ) " +
                                "       INNER JOIN M_PRODUCT        MPO ON (MPO.M_PRODUCT_ID = PRP.M_PRODUCT_ID) " +
                                "              INNER JOIN M_STORAGE	MS ON (MS.M_PRODUCT_ID = MPO.M_PRODUCT_ID ) " +
                                "   WHERE PRP.M_PRODUCT_ID IN (SELECT M_PRODUCT_ID  FROM M_PRODUCT_PO WHERE MPO.M_PRODUCT_ID IN ( " + sArticuloID + " )) " +
                                "       AND MPO.AD_CLIENT_ID = " + iClienteID + " " + //AND MPO.AD_ORG_ID = " + iOrganizacionID + " " +
                                "       AND PRP.XX_LO_PALLET_QTY_EXISTS > 0 " +
                                "       AND PRP.XX_LO_INVOICED = 'N' " +
                                "       AND LOC.VALUE <> 'V00-000-00-00-00' " +
                                "       AND PR.XX_LO_ISDROPSHIP        = 'N' " +
                                "       AND PRP.XX_LO_PALLET_QTY_EXCESS = 0 " +
                                "   UNION ALL " +
                                "   SELECT LOC.VALUE AS UBICACION " +
                                "       , PPA.XX_LO_PROCESS_PALLET_LICENSE AS PALETA " +
                                "       , PPL.XX_LO_PURCHASERECORD_PALLET_LICENSE AS PALETA_ORIGEN " +
                                "       , MPO.VALUE AS CODIGO_OLO " +
                                "       , MPO.M_PRODUCT_Id " +
                                "       , PPL.XX_LO_ONHAND AS EXISTENCIA_PALETA " +
                                "       , MS.QTYONHAND AS EXISTENCIA_TOTAL " +
                                "       , PPA.M_WAREHOUSE_ID " +
                                "       , LOC.M_LOCATOR_ID " +
                                "       , PPL.XX_LO_PURCHASERECORDNO AS XX_LO_PURCHASERECORDNO	" +
                                "		, PR.XX_LO_FINALSTATUS AS XX_LO_FINALSTATUS	" +
                                "   FROM XX_LO_PROCESS_PALLET_LINE PPL " +
                                "       INNER JOIN XX_LO_PROCESS_PALLET        PPA ON (PPA.XX_LO_PROCESS_PALLET_ID = PPL.XX_LO_PROCESS_PALLET_ID) " +
                                "       LEFT JOIN (XX_LO_PURCHASERECORD_PALLET PRP " +
                                "               INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO)    ) " +
                                "           ON (PRP.XX_LO_PALLET_LICENSE = PPL.XX_LO_PURCHASERECORD_PALLET_LICENSE AND PRP.M_PRODUCT_ID = PPL.M_PRODUCT_ID) " +
                                "       INNER JOIN M_LOCATOR                   LOC ON (LOC.M_LOCATOR_ID = PPA.M_LOCATOR_ID AND SUBSTR(LOC.VALUE, 0, 2) IN ('0') AND SUBSTR(LOC.VALUE, 9, 2) IN ('01')) " +
                                "       INNER JOIN M_PRODUCT MPO ON (MPO.M_PRODUCT_ID = PPL.M_PRODUCT_ID) " +
                                "              INNER JOIN M_STORAGE	MS ON (MS.M_PRODUCT_ID = MPO.M_PRODUCT_ID ) " +
                                "   WHERE PPL.M_PRODUCT_ID IN (SELECT M_PRODUCT_ID FROM M_PRODUCT_PO WHERE MPO.M_PRODUCT_ID IN ( " + sArticuloID + " )) " +
                                "       AND MPO.AD_CLIENT_ID = " + iClienteID + " " + //AND MPO.AD_ORG_ID = " + iOrganizacionID + " " +
                                "       AND PPL.XX_LO_ONHAND > 0 " +
                                "       AND PPA.XX_LO_INVOICED = 'N' " +
                                "       AND LOC.VALUE <> 'V00-000-00-00-00' " +
                                ") AS RESULT_UBICA " +
                                "ORDER BY UBICACION ASC ";

                        dsPaletas = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        throw e;
                    }
                }

                /// <summary>
                /// Carga las paletas para un artículo en particular. 
                /// Que se encuentre en almacen - Rack, inter pasillo, jaula Aforo, jaula rebaja cero y área de recolección.
                /// </summary>
                /// <returns></returns>
                public void Consultar_Paletas_Almacen(String sArticuloID, out DataSet dsPaletas, int iOrganizacionID, int iClienteID)
            {
                Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                OleDbCommand CmdSelect = new OleDbCommand();

                try
                {

                    CmdSelect.CommandText =
                           "SELECT *  FROM ( " +
                            "   SELECT LOC.VALUE UBICACION " +
                            "       , PRP.XX_LO_PALLET_LICENSE PALETA " +
                            "       , PRP.XX_LO_PALLET_LICENSE  AS PALETA_ORIGEN " +
                            "       , MPO.VALUE AS CODIGO_OLO " +
                            "       , MPO.M_PRODUCT_ID AS M_PRODUCT_ID " +
                            "       , PRP.XX_LO_PALLET_QTY_EXISTS AS EXISTENCIA_PALETA " +
                            "       , MS.QTYONHAND AS EXISTENCIA_TOTAL " +
                            "       , PRP.M_WAREHOUSE_ID " +
                            "       , LOC.M_LOCATOR_ID " +
                            "       , PRP.XX_LO_PURCHASERECORDNO AS XX_LO_PURCHASERECORDNO	" +
                            "		, PR.XX_LO_FINALSTATUS AS XX_LO_FINALSTATUS	" +
                            "   FROM XX_LO_PURCHASERECORD_PALLET PRP " +
                            "       INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO) " +
                            "       INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PRP.M_LOCATOR_ID AND (SUBSTR(LOC.VALUE, 0, 2) IN ('0', 'A', 'R') OR SUBSTR(LOC.VALUE, 0, 4) = 'ZPK' )  ) " +
                            "       INNER JOIN M_PRODUCT        MPO ON (MPO.M_PRODUCT_ID = PRP.M_PRODUCT_ID) " +
                            "              INNER JOIN M_STORAGE	MS ON (MS.M_PRODUCT_ID = MPO.M_PRODUCT_ID ) " +
                            "   WHERE PRP.M_PRODUCT_ID IN (SELECT M_PRODUCT_ID  FROM M_PRODUCT_PO WHERE MPO.M_PRODUCT_ID IN ( " + sArticuloID + " )) " +
                            "       AND MPO.AD_CLIENT_ID = " + iClienteID + " " + //AND MPO.AD_ORG_ID = " + iOrganizacionID + " " +
                            "       AND PRP.XX_LO_PALLET_QTY_EXISTS > 0 " +
                            "       AND PRP.XX_LO_INVOICED = 'N' " +
                            "       AND LOC.VALUE <> 'V00-000-00-00-00' " +
                            "       AND PR.XX_LO_ISDROPSHIP        = 'N' " +
                            "       AND PRP.XX_LO_PALLET_QTY_EXCESS = 0 " +
                            "   UNION ALL " +
                            "   SELECT LOC.VALUE AS UBICACION " +
                            "       , PPA.XX_LO_PROCESS_PALLET_LICENSE AS PALETA " +
                            "       , PPL.XX_LO_PURCHASERECORD_PALLET_LICENSE AS PALETA_ORIGEN " +
                            "       , MPO.VALUE AS CODIGO_OLO " +
                            "       , MPO.M_PRODUCT_Id " +
                            "       , PPL.XX_LO_ONHAND AS EXISTENCIA_PALETA " +
                            "       , MS.QTYONHAND AS EXISTENCIA_TOTAL " +
                            "       , PPA.M_WAREHOUSE_ID " +
                            "       , LOC.M_LOCATOR_ID " +
                            "       , PPL.XX_LO_PURCHASERECORDNO AS XX_LO_PURCHASERECORDNO	" +
                            "		, PR.XX_LO_FINALSTATUS AS XX_LO_FINALSTATUS	" +
                            "   FROM XX_LO_PROCESS_PALLET_LINE PPL " +
                            "       INNER JOIN XX_LO_PROCESS_PALLET        PPA ON (PPA.XX_LO_PROCESS_PALLET_ID = PPL.XX_LO_PROCESS_PALLET_ID) " +
                            "       LEFT JOIN (XX_LO_PURCHASERECORD_PALLET PRP " +
                            "               INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO)    ) " +
                            "           ON (PRP.XX_LO_PALLET_LICENSE = PPL.XX_LO_PURCHASERECORD_PALLET_LICENSE AND PRP.M_PRODUCT_ID = PPL.M_PRODUCT_ID) " +
                            "       INNER JOIN M_LOCATOR                   LOC ON (LOC.M_LOCATOR_ID = PPA.M_LOCATOR_ID AND (SUBSTR(LOC.VALUE, 0, 2) IN ('0', 'A', 'R') OR SUBSTR(LOC.VALUE, 0, 4) = 'ZPK' ) ) " +
                            "       INNER JOIN M_PRODUCT MPO ON (MPO.M_PRODUCT_ID = PPL.M_PRODUCT_ID) " +
                            "              INNER JOIN M_STORAGE	MS ON (MS.M_PRODUCT_ID = MPO.M_PRODUCT_ID ) " +
                            "   WHERE PPL.M_PRODUCT_ID IN (SELECT M_PRODUCT_ID FROM M_PRODUCT_PO WHERE MPO.M_PRODUCT_ID IN ( " + sArticuloID + " )) " +
                            "       AND MPO.AD_CLIENT_ID = " + iClienteID + " " + //AND MPO.AD_ORG_ID = " + iOrganizacionID + " " +
                            "       AND PPL.XX_LO_ONHAND > 0 " +
                            "       AND PPA.XX_LO_INVOICED = 'N' " +
                            "       AND LOC.VALUE <> 'V00-000-00-00-00' " +
                            ") AS RESULT_UBICA " +
                            "ORDER BY UBICACION ASC ";

                    dsPaletas = Conexion.executeCmdDataSet(CmdSelect);

                }
                catch (Exception e)
                {
                    throw e;
                }
            }

                /// <summary>
                /// carga datos - Existencias en uno
                /// </summary>
                /// <returns></returns>
                public Boolean Cargar_Existencias_En_Uno(out DataSet dsExistenciasEnUno, int iOrganizacionID = 1000230, int iClienteID = 1000000)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {

                        CmdSelect.CommandText =
                               "SELECT S.M_PRODUCT_ID AS M_PRODUCT_ID " +
                                " 		    , (SELECT T.VENDORPRODUCTNO FROM XX_LO_TRANSACCION_INV T WHERE S.M_PRODUCT_ID=T.M_PRODUCT_ID AND T.QTY_INVENTORY = 1 ORDER BY CREATED DESC FETCH FIRST 1 ROWS ONLY) AS XX_LO_REFERENCIA " +
                                " 		    , S.QTYONHAND AS  XX_LO_EXISTENCIA_SISTEMA " +
                                " 		    , 'EU' AS TIPO_CONTEO " +
                                "           ,  0  AS CANTIDAD_PALETAS " +
                                "        	FROM M_STORAGE AS S " +
                                "        	WHERE S.QTYONHAND=1 " +
                                "        		AND S.AD_CLIENT_ID = " + iClienteID + " " +
                                //"        		AND S.AD_ORG_ID = " + iOrganizacionID + " " +
                                "			AND S.M_PRODUCT_ID NOT IN (SELECT CD.M_PRODUCT_ID " +
                                "									FROM XX_LO_CABECERA_CONTEO_DIARIO AS CD " +
                                "									WHERE TIMESTAMPDIFF(16, CHAR(GETDATE()-CD.CREATED)) <=  (SELECT COUNT(C.M_PRODUCT_ID) FROM M_STORAGE AS C WHERE C.QTYONHAND=1 AND C.AD_CLIENT_ID = 1000000 AND C.AD_ORG_ID = 1000230)  ) " +
                                "		ORDER BY S.UPDATED DESC ";

                        dsExistenciasEnUno = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        bResultado = false;
                        throw e;
                    }
                    return bResultado;
                }

                /// <summary>
                /// carga datos - Existencias negativas
                /// </summary>
                /// <returns></returns>
                public Boolean Cargar_Existencias_Negativas(out DataSet dsExistenciasNegativas, int iOrganizacionID = 1000230, int iClienteID = 1000000)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {

                        CmdSelect.CommandText =
                               "        SELECT S.M_PRODUCT_ID AS M_PRODUCT_ID " +
                                " 		    , (SELECT T.VENDORPRODUCTNO FROM XX_LO_TRANSACCION_INV T WHERE S.M_PRODUCT_ID=T.M_PRODUCT_ID AND T.QTY_INVENTORY < 0 ORDER BY CREATED DESC FETCH FIRST 1 ROWS ONLY) AS XX_LO_REFERENCIA " +
                                " 		    , S.QTYONHAND AS  XX_LO_EXISTENCIA_SISTEMA " +
                                " 		    , 'EN' AS TIPO_CONTEO " +
                                " 		    , 0 AS CANTIDAD_PALETAS " +
                                "        	FROM M_STORAGE AS S " +
                                "        	WHERE S.QTYONHAND < 0 " +
                                "        		AND S.AD_CLIENT_ID = " + iClienteID + " " +
                                //"        		AND S.AD_ORG_ID = " + iOrganizacionID + " " +
                                "			AND S.M_PRODUCT_ID NOT IN (SELECT CD.M_PRODUCT_ID " +
                                "									FROM XX_LO_CABECERA_CONTEO_DIARIO AS CD " +
                                "									WHERE TIMESTAMPDIFF(16, CHAR(GETDATE()-CD.CREATED)) <=  (SELECT COUNT(C.M_PRODUCT_ID) FROM M_STORAGE AS C WHERE C.QTYONHAND < 0 AND C.AD_CLIENT_ID = 1000000 AND C.AD_ORG_ID = 1000230)  ) " +
                                "		ORDER BY S.UPDATED DESC ";

                        dsExistenciasNegativas = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        bResultado = false;
                        throw e;
                    }
                    return bResultado;
                }

                /// <summary>
                /// carga datos - Mayor valorizado por recolección
                /// </summary>
                /// <returns></returns>
                public Boolean Cargar_Mayor_Valorizado_Recoleccion(String sFechaInicial, String sFechaFinal, out DataSet dsExistencias, int iOrganizacionID = 1000230, int iClienteID = 1000000)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {

                        CmdSelect.CommandText =
                                " SELECT T.M_PRODUCT_ID AS M_PRODUCT_ID " +
                                " 		     , T.VENDORPRODUCTNO AS XX_LO_REFERENCIA " +
                                " 		     , S.QTYONHAND AS  XX_LO_EXISTENCIA_SISTEMA " +
                                " 			, 'VR' AS TIPO_CONTEO " +
                                " 			, 0 AS CANTIDAD_PALETAS " +
                                " 	    FROM XX_LO_TRANSACCION_INV AS T " +
                                " 		        INNER JOIN M_STORAGE AS S ON ( S.M_PRODUCT_ID=T.M_PRODUCT_ID AND S.QTYONHAND>0 AND T.XX_LO_DOCTYPE='FA' ) " +
                                " 	    WHERE T.DATE_DOCUMENT BETWEEN '" + sFechaInicial + "' AND '" + sFechaFinal + "' " +
                                "               AND T.AD_CLIENT_ID = " + iClienteID + " " + //AND T.AD_ORG_ID = " + iOrganizacionID + " " +
                                " 	    		AND T.XX_LO_PURCHASERECORDNO IS NULL 	AND T.XX_LO_PO_ORIGINAL IS NULL " +
                                " 		     AND T.ISDROPSHIP='N' AND T.E_S='S' " +
                                "               AND 0 < (SELECT  SUM(CANTIDAD) AS EN_PALETAS FROM ( " +
                                " 					SELECT PRP.M_PRODUCT_ID, XX_LO_PALLET_QTY_EXISTS AS CANTIDAD " +
                                " 						FROM   XX_LO_PURCHASERECORD_PALLET PRP INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO) " +
                                "                                   INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PRP.M_LOCATOR_ID AND SUBSTR(LOC.VALUE, 0, 2) NOT IN ('X', 'D')  ) " +
                                " 					     WHERE PRP.XX_LO_PALLET_QTY_EXISTS > 0 AND PRP.XX_LO_INVOICED = 'N' AND PR.XX_LO_ISDROPSHIP = 'N' AND PRP.XX_LO_PALLET_QTY_EXCESS = 0 " +
                                " 					UNION " +
                                " 					 SELECT PPL.M_PRODUCT_ID, XX_LO_ONHAND AS CANTIDAD " +
                                " 					 	FROM XX_LO_PROCESS_PALLET_LINE PPL INNER JOIN XX_LO_PROCESS_PALLET  PPA ON (PPA.XX_LO_PROCESS_PALLET_ID = PPL.XX_LO_PROCESS_PALLET_ID) " +
                                "                                   INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PPA.M_LOCATOR_ID AND SUBSTR(LOC.VALUE, 0, 2) NOT IN ('X', 'D')  ) " +
                                " 					     WHERE PPL.XX_LO_ONHAND > 0 AND PPA.XX_LO_INVOICED = 'N' " +
                                " 					     )  AS EXISTENCIA_PALETAS WHERE M_PRODUCT_ID = T.M_PRODUCT_ID) " +
                                " 	    GROUP BY T.M_PRODUCT_ID, S.QTYONHAND , T.VENDORPRODUCTNO " +
                                " 	    ORDER BY MAX(COST_UNIT) DESC  ";

                        dsExistencias = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        bResultado = false;
                        throw e;
                    }
                    return bResultado;
                }

                /// <summary>
                /// carga datos - Movimiento por recolección
                /// </summary>
                /// <returns></returns>
                public Boolean Cargar_Movimiento_Recoleccion(String sFechaInicial, String sFechaFinal, out DataSet dsExistencias, int iOrganizacionID = 1000230, int iClienteID = 1000000 )
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {

                        CmdSelect.CommandText =
                                "SELECT T.M_PRODUCT_ID AS M_PRODUCT_ID " +
                                " 		     , T.VENDORPRODUCTNO AS XX_LO_REFERENCIA " +
                                " 		     , (SELECT  SUM(CANTIDAD) AS EN_PALETAS FROM ( " +
                                " 					SELECT PRP.M_PRODUCT_ID, XX_LO_PALLET_QTY_EXISTS AS CANTIDAD " +
                                " 						FROM   XX_LO_PURCHASERECORD_PALLET PRP INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO) " +
                                " 								INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PRP.M_LOCATOR_ID AND SUBSTR(LOC.VALUE, 0, 2) IN ('0') AND SUBSTR(LOC.VALUE, 9, 2) IN ('01') ) " +
                                " 					     WHERE PRP.XX_LO_PALLET_QTY_EXISTS > 0 AND PRP.XX_LO_INVOICED = 'N' AND PR.XX_LO_ISDROPSHIP = 'N' AND PRP.XX_LO_PALLET_QTY_EXCESS = 0 " +
                                " 					UNION ALL " +
                                " 					 SELECT PPL.M_PRODUCT_ID, XX_LO_ONHAND AS CANTIDAD " +
                                " 					 	FROM XX_LO_PROCESS_PALLET_LINE PPL INNER JOIN XX_LO_PROCESS_PALLET  PPA ON (PPA.XX_LO_PROCESS_PALLET_ID = PPL.XX_LO_PROCESS_PALLET_ID) " +
                                " 					 		INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PPA.M_LOCATOR_ID AND SUBSTR(LOC.VALUE, 0, 2) IN ('0') AND SUBSTR(LOC.VALUE, 9, 2) IN ('01')  ) " +
                                " 					     WHERE PPL.XX_LO_ONHAND > 0 AND PPA.XX_LO_INVOICED = 'N' " +
                                " 					     )  AS EXISTENCIA_PALETAS WHERE M_PRODUCT_ID = T.M_PRODUCT_ID)	 AS  XX_LO_EXISTENCIA_SISTEMA " +
                                " 			, 'MR' AS TIPO_CONTEO " +
                                " 			, 0 AS CANTIDAD_PALETAS " +
                                " 	    FROM XX_LO_TRANSACCION_INV AS T " +
                                " 		        INNER JOIN M_STORAGE AS S ON ( S.M_PRODUCT_ID=T.M_PRODUCT_ID AND S.QTYONHAND>0 AND T.XX_LO_DOCTYPE='FA' ) " +
                                " 	    WHERE T.DATE_DOCUMENT BETWEEN '"+ sFechaInicial + "' AND '" + sFechaFinal + "' " +
                                "               AND T.AD_CLIENT_ID = " + iClienteID + " " + //AND T.AD_ORG_ID = " + iOrganizacionID + " " +
                                " 	    		AND T.XX_LO_PURCHASERECORDNO IS NULL 	AND T.XX_LO_PO_ORIGINAL IS NULL " +
                                " 		     AND T.ISDROPSHIP='N' AND T.E_S='S' " +
                                "               AND 0 < (SELECT  SUM(CANTIDAD) AS EN_PALETAS FROM ( " +
                                " 					SELECT PRP.M_PRODUCT_ID, XX_LO_PALLET_QTY_EXISTS AS CANTIDAD " +
                                " 						FROM   XX_LO_PURCHASERECORD_PALLET PRP INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO) " +
                                " 								INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PRP.M_LOCATOR_ID AND SUBSTR(VALUE, 0, 2) IN ('0') AND SUBSTR(LOC.VALUE, 9, 2) IN ('01') ) " +
                                " 					     WHERE PRP.XX_LO_PALLET_QTY_EXISTS > 0 AND PRP.XX_LO_INVOICED = 'N' AND PR.XX_LO_ISDROPSHIP = 'N' AND PRP.XX_LO_PALLET_QTY_EXCESS = 0 " +
                                " 					UNION ALL " +
                                " 					 SELECT PPL.M_PRODUCT_ID, XX_LO_ONHAND AS CANTIDAD " +
                                " 					 	FROM XX_LO_PROCESS_PALLET_LINE PPL INNER JOIN XX_LO_PROCESS_PALLET  PPA ON (PPA.XX_LO_PROCESS_PALLET_ID = PPL.XX_LO_PROCESS_PALLET_ID) " +
                                " 					 		INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PPA.M_LOCATOR_ID AND SUBSTR(LOC.VALUE, 0, 2) IN ('0') AND SUBSTR(LOC.VALUE, 9, 2) IN ('01')  ) " +
                                " 					     WHERE PPL.XX_LO_ONHAND > 0 AND PPA.XX_LO_INVOICED = 'N' " +
                                " 					     )  AS EXISTENCIA_PALETAS WHERE M_PRODUCT_ID = T.M_PRODUCT_ID) " +
                                " 	    GROUP BY T.M_PRODUCT_ID, S.QTYONHAND, T.VENDORPRODUCTNO ";

                        dsExistencias = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        bResultado = false;
                        throw e;
                    }
                    return bResultado;
                }

                /// <summary>
                /// carga datos - Mayor valorizado recibido
                /// </summary>
                /// <returns></returns>
                public Boolean Cargar_Mayor_Valorizado_Recibido(String sFechaInicial, String sFechaFinal, out DataSet dsExistencias, int iOrganizacionID = 1000230, int iClienteID = 1000000)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {

                        CmdSelect.CommandText =
                                "SELECT T.M_PRODUCT_ID AS M_PRODUCT_ID " +
                                " 		     , T.VENDORPRODUCTNO AS XX_LO_REFERENCIA " +
                                " 			, S.QTYONHAND AS XX_LO_EXISTENCIA_SISTEMA " +
                                " 			, 'RE' AS TIPO_CONTEO " +
                                " 			, 0 AS CANTIDAD_PALETAS " +
                                " 	    FROM XX_LO_TRANSACCION_INV AS T " +
                                " 		        INNER JOIN M_STORAGE AS S ON ( S.M_PRODUCT_ID=T.M_PRODUCT_ID AND S.QTYONHAND>0 AND T.XX_LO_DOCTYPE='NR' AND T.ISDROPSHIP='N' AND T.E_S='E' ) " +
                                " 	    WHERE T.DATE_DOCUMENT BETWEEN '" + sFechaInicial + "' AND '" + sFechaFinal + "' " +
                                "               AND T.AD_CLIENT_ID = " + iClienteID + " " + //AND T.AD_ORG_ID = " + iOrganizacionID + " " +
                                "               AND 0 < (SELECT  SUM(CANTIDAD) AS EN_PALETAS FROM ( " +
                                " 					SELECT PRP.M_PRODUCT_ID, XX_LO_PALLET_QTY_EXISTS AS CANTIDAD " +
                                " 						FROM   XX_LO_PURCHASERECORD_PALLET PRP INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO) " +
                                "                                   INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PRP.M_LOCATOR_ID AND SUBSTR(LOC.VALUE, 0, 2) NOT IN ('X', 'D')  ) " +
                                " 					     WHERE PRP.XX_LO_PALLET_QTY_EXISTS > 0 AND PRP.XX_LO_INVOICED = 'N' AND PR.XX_LO_ISDROPSHIP = 'N' AND PRP.XX_LO_PALLET_QTY_EXCESS = 0 " +
                                " 					UNION ALL " +
                                " 					 SELECT PPL.M_PRODUCT_ID, XX_LO_ONHAND AS CANTIDAD " +
                                " 					 	FROM XX_LO_PROCESS_PALLET_LINE PPL INNER JOIN XX_LO_PROCESS_PALLET  PPA ON (PPA.XX_LO_PROCESS_PALLET_ID = PPL.XX_LO_PROCESS_PALLET_ID) " +
                                "                                   INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PPA.M_LOCATOR_ID AND SUBSTR(LOC.VALUE, 0, 2) NOT IN ('X', 'D')  ) " +
                                " 					     WHERE PPL.XX_LO_ONHAND > 0 AND PPA.XX_LO_INVOICED = 'N' " +
                                " 					     )  AS EXISTENCIA_PALETAS WHERE M_PRODUCT_ID = T.M_PRODUCT_ID) " +
                                " 	    GROUP BY T.M_PRODUCT_ID, S.QTYONHAND, T.VENDORPRODUCTNO " +
                                " 	    ORDER BY MAX(COST_UNIT) DESC ";

                        dsExistencias = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        bResultado = false;
                        throw e;
                    }
                    return bResultado;
                }

                /// <summary>
                /// carga datos - Lento movimiento
                /// </summary>
                /// <returns></returns>
                public Boolean Cargar_Lento_Movimiento(out DataSet dsExistencias, int iOrganizacionID = 1000230, int iClienteID = 1000000)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {

                        CmdSelect.CommandText =
                                "SELECT T.M_PRODUCT_ID AS M_PRODUCT_ID " +
                                " 		     , T.VENDORPRODUCTNO AS XX_LO_REFERENCIA " +
                                " 		     , (SELECT  SUM(CANTIDAD) AS EN_PALETAS FROM ( " +
                                " 					SELECT PRP.M_PRODUCT_ID, XX_LO_PALLET_QTY_EXISTS AS CANTIDAD " +
                                " 						FROM   XX_LO_PURCHASERECORD_PALLET PRP INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO) " +
                                " 								INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PRP.M_LOCATOR_ID AND (SUBSTR(LOC.VALUE, 0, 2) IN ('0', 'A', 'R') OR SUBSTR(LOC.VALUE, 0, 4) = 'ZPK')) " +
                                " 					     WHERE PRP.XX_LO_PALLET_QTY_EXISTS > 0 AND PRP.XX_LO_INVOICED = 'N' AND PR.XX_LO_ISDROPSHIP = 'N' AND PRP.XX_LO_PALLET_QTY_EXCESS = 0 " +
                                "                               AND PRP.AD_CLIENT_ID = " + iClienteID + " "+ //AND PRP.AD_ORG_ID = " + iOrganizacionID + " " +
                                " 					UNION ALL " +
                                " 					 SELECT PPL.M_PRODUCT_ID, XX_LO_ONHAND AS CANTIDAD " +
                                " 					 	FROM XX_LO_PROCESS_PALLET_LINE PPL INNER JOIN XX_LO_PROCESS_PALLET  PPA ON (PPA.XX_LO_PROCESS_PALLET_ID = PPL.XX_LO_PROCESS_PALLET_ID) " +
                                " 					 		INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PPA.M_LOCATOR_ID AND (SUBSTR(LOC.VALUE, 0, 2) IN ('0', 'A', 'R') OR SUBSTR(LOC.VALUE, 0, 4) = 'ZPK')) " +
                                " 					     WHERE PPL.XX_LO_ONHAND > 0 AND PPA.XX_LO_INVOICED = 'N' " +
                                "                               AND PPL.AD_CLIENT_ID = " + iClienteID + " "+ //AND PPL.AD_ORG_ID = " + iOrganizacionID + " " +
                                " 					     )  AS EXISTENCIA_PALETAS WHERE M_PRODUCT_ID = T.M_PRODUCT_ID)	 AS  XX_LO_EXISTENCIA_SISTEMA " +
                                " 			, 'LM' AS TIPO_CONTEO " +
                                "			, 0 AS CANTIDAD_PALETAS " +
                                " 	    FROM XX_LO_TRANSACCION_INV AS T " +
                                " 		        INNER JOIN M_STORAGE AS S ON ( S.M_PRODUCT_ID=T.M_PRODUCT_ID AND S.QTYONHAND>0 AND T.XX_LO_DOCTYPE='FA' AND T.ISDROPSHIP='N' AND T.E_S='S') " +
                                " 	    WHERE  T.AD_CLIENT_ID = " + iClienteID + " "+ //AND T.AD_ORG_ID = " + iOrganizacionID + " " +
                                "               AND T.XX_LO_TRANSACCION_INV_ID IN ( SELECT R.XX_LO_TRANSACCION_INV_ID " +
                                "					FROM ( SELECT MAX(TMP.XX_LO_TRANSACCION_INV_ID) AS XX_LO_TRANSACCION_INV_ID, MAX(TMP.CREATED) AS CREATED, TMP.M_PRODUCT_ID " +
                                "							FROM XX_LO_TRANSACCION_INV AS TMP " +
                                "							WHERE  TMP.XX_LO_DOCTYPE='FA' AND TMP.ISDROPSHIP='N' AND TMP.E_S='S' GROUP BY TMP.M_PRODUCT_ID " +
                                "						) AS R WHERE TIMESTAMPDIFF(16, CHAR(GETDATE()-R.CREATED)) > 180 AND  R.M_PRODUCT_ID = T.M_PRODUCT_ID   ) " +
                                " 		     AND 0 < (SELECT  SUM(CANTIDAD) AS EN_PALETAS FROM ( " +
                                " 					SELECT PRP.M_PRODUCT_ID, XX_LO_PALLET_QTY_EXISTS AS CANTIDAD " +
                                " 						FROM   XX_LO_PURCHASERECORD_PALLET PRP INNER JOIN XX_LO_PURCHASERECORD PR ON (PRP.XX_LO_PURCHASERECORDNO=PR.XX_LO_PURCHASERECORDNO) " +
                                " 								INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PRP.M_LOCATOR_ID AND (SUBSTR(LOC.VALUE, 0, 2) IN ('0', 'A', 'R') OR SUBSTR(LOC.VALUE, 0, 4) = 'ZPK')) " +
                                " 					     WHERE PRP.XX_LO_PALLET_QTY_EXISTS > 0 AND PRP.XX_LO_INVOICED = 'N' AND PR.XX_LO_ISDROPSHIP = 'N' AND PRP.XX_LO_PALLET_QTY_EXCESS = 0 " +
                                " 					UNION ALL " +
                                " 					 SELECT PPL.M_PRODUCT_ID, XX_LO_ONHAND AS CANTIDAD " +
                                " 					 	FROM XX_LO_PROCESS_PALLET_LINE PPL INNER JOIN XX_LO_PROCESS_PALLET  PPA ON (PPA.XX_LO_PROCESS_PALLET_ID = PPL.XX_LO_PROCESS_PALLET_ID) " +
                                " 					 		INNER JOIN M_LOCATOR        LOC ON (LOC.M_LOCATOR_ID = PPA.M_LOCATOR_ID AND (SUBSTR(LOC.VALUE, 0, 2) IN ('0', 'A', 'R') OR SUBSTR(LOC.VALUE, 0, 4) = 'ZPK')) " +
                                " 					     WHERE PPL.XX_LO_ONHAND > 0 AND PPA.XX_LO_INVOICED = 'N' " +
                                " 					     )  AS EXISTENCIA_PALETAS WHERE M_PRODUCT_ID = T.M_PRODUCT_ID) " +
                                " 	    GROUP BY T.M_PRODUCT_ID, S.QTYONHAND, T.VENDORPRODUCTNO " +
                                " 	    ORDER BY MAX(T.CREATED) DESC ";

                        dsExistencias = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        bResultado = false;
                        throw e;
                    }
                    return bResultado;
                }

                /// <summary>
                /// carga datos - Ubicaciones vacias
                /// </summary>
                /// <returns></returns>
                public Boolean Cargar_Ubicaciones_Vacias(out DataSet dsExistencias)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {

                        CmdSelect.CommandText =
                                " SELECT 00 AS M_PRODUCT_ID " +
                                " 		     , '--' AS XX_LO_REFERENCIA " +
                                " 			, COUNT(VALUE) AS XX_LO_EXISTENCIA_SISTEMA " +
                                " 			, 'UV' AS TIPO_CONTEO " +
                                "     FROM XX_LO_GLOBAL_LOCATOR " +
                                "    WHERE VALUE_COMPANY = '--' " +
                                "      AND VALUE NOT IN ('N00-000-00-00-00', 'V00-000-00-00-00', 'E00-000-00-00-00') " +
                                "      AND ISACTIVE                 = 'Y' " +
                                "      AND ISAVAILABLEFORALLOCATION = 'Y' " +
                                " GROUP BY VALUE_COMPANY ";

                        dsExistencias = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        bResultado = false;
                        throw e;
                    }
                    return bResultado;
                }

                /// <summary>
                /// carga datos - Conteo Diario 
                /// </summary>
                /// <returns></returns>
                public Boolean Cargar_Articulos_Conteo_Diario(String sFechaConteo, out DataSet dsExistencias, int iOrganizacionID = 1000230, int iClienteID = 1000000)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {
                        //sFechaConteo ="2017-03-01 00:00:00";
                        CmdSelect.CommandText =
                                " SELECT XX_LO_CONTEO_DIARIO_ID " +
                                "	, M_PRODUCT_ID	, XX_LO_EXISTENCIA_SISTEMA " +
                                "	, TIPO_CONTEO	, XX_LO_FECHA_CONTEO_DIARIO " +
                                " FROM XX_LO_CABECERA_CONTEO_DIARIO " +
                                " WHERE XX_LO_FECHA_CONTEO_DIARIO = '" + sFechaConteo + "' "+
                                "       AND AD_CLIENT_ID   = " + iClienteID + " " +
                                "       AND AD_ORG_ID = " + iOrganizacionID + " ";

                        dsExistencias = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        bResultado = false;
                        throw e;
                    }
                    return bResultado;
                }

                /// <summary>
                /// carga datos - Clientes de la compañia 
                /// </summary>
                /// <returns></returns>
                public Boolean Cargar_Clientes_Compania(String sCompania, out DataSet dsClientes)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {
                        //, ADO.NAME AS ORGANIZACION
                        CmdSelect.CommandText =
                                " SELECT ADC.AD_CLIENT_ID AS CLIENTE_ID, ADC.NAME AS CLIENTE, ADO.AD_ORG_ID AS ORGANIZACION_ID " +
                                            " FROM AD_CLIENT ADC " +
                                            " 	INNER JOIN AD_ORG ADO ON (ADO.AD_CLIENT_ID = ADC.AD_CLIENT_ID) " +
                                            " WHERE ADC.AD_CLIENT_ID > 1000000 AND ADC.ISACTIVE = 'Y' AND ADO.ISSUMMARY = 'N' ";
                        
                        dsClientes = Conexion.executeCmdDataSet(CmdSelect);

                    }
                    catch (Exception e)
                    {
                        bResultado = false;
                        throw e;
                    }
                    return bResultado;
                }

            #endregion

            #region Calculos

                /// <summary>
                /// Calculo de muestra para tipo de conteo - Tabla Militar
                /// </summary>
                /// <returns></returns>
                public Int32 Calcular_Muestra(Int32 iPoblacion, String sNivelInspeccion, String sTipoMuestra)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdSelect = new OleDbCommand();
                    DataSet dsMuestra = null;
                    String _sNivelInspeccion, _sTipoMuestra;
                    Int32 iTamanoMuestra = 1;
                    try
                    {
                        if (sNivelInspeccion.Equals("I"))
                        {
                            _sNivelInspeccion = "XX_LO_NIVEL_INSPECCION_I";
                        }
                        else
                        {//II
                            _sNivelInspeccion = "XX_LO_NIVEL_INSPECCION_II";
                        }

                        if (sTipoMuestra.Equals("Normal"))
                        {
                            _sTipoMuestra = "XX_LO_MUESTRA_NORMAL";
                        }
                        else
                        {//Ligera
                            _sTipoMuestra = "XX_LO_MUESTRA_LIGERA";
                        }

                        CmdSelect.CommandText =
                               " SELECT M." + _sTipoMuestra + " AS MUESTRA FROM XX_LO_CONTEO_DIARIO_NIVEL_INSPECCION I " +
                                    " INNER JOIN XX_LO_CONTEO_DIARIO_MUESTRA_INSPECCION M " +
                                        " ON I." + _sNivelInspeccion + " = M.XX_LO_CODIGO_MUESTRA " +
                                        " AND I.XX_LO_ESTADO = 'A' " +
                                        " AND " + iPoblacion + " BETWEEN I.XX_LO_POBLACION_MINIMA AND I.XX_LO_POBLACION_MAXIMA ";

                        dsMuestra = Conexion.executeCmdDataSet(CmdSelect);

                        foreach (DataRow r in dsMuestra.Tables[0].Rows)
                        {
                            iTamanoMuestra = Int32.Parse(r["MUESTRA"].ToString());
                        }
                    }
                    catch (Exception e)
                    {
                        throw e;
                    }
                    return iTamanoMuestra;
                }
            #endregion

            #region Insertar, Actualizar, Eliminar

                /// <summary>
                /// insertar datos - detalle de Conteo Diario 
                /// </summary>
                /// <returns></returns>
                public Boolean Insertar_Detalle_Conteo_Diario(String sConteoDiarioID, String sWareHouseID, String sLocatorID, String sPaleta, String sPurchaseRecordNO
                                                                    , String sFinalStatus, String sPaletaOrigen, String sExistenciaPaleta, int iOrganizacionID = 1000230, int iClienteID = 1000000)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdInsert = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {
                        CmdInsert.CommandText =
                                " INSERT INTO XX_LO_DETALLE_CONTEO_DIARIO (AD_CLIENT_ID, AD_ORG_ID , CREATED , CREATEDBY , ISACTIVE , UPDATED , UPDATEDBY  " +
                                "    , XX_LO_CONTEO_DIARIO_ID , M_WAREHOUSE_ID , M_LOCATOR_ID , XX_LO_PALLET_LICENSE " +
                                "    , XX_LO_PURCHASERECORDNO , XX_LO_FINALSTATUS , XX_LO_PURCHASERECORD_PALLET_LICENSE , XX_LO_QTY_ONPALLET ) " +
                                "	VALUES( " + iClienteID + " , " + iOrganizacionID + " , CURRENT_TIMESTAMP , 0 , 'Y' , CURRENT_TIMESTAMP , 0 " +
                                "               , " + sConteoDiarioID + " , " + sWareHouseID + " , " + sLocatorID + " , '" + sPaleta + "' " +
                                "               , " + sPurchaseRecordNO + " , '" + sFinalStatus + "' , '" + sPaletaOrigen + "' , " + sExistenciaPaleta + " ) ";

                        bResultado = Conexion.executeCmdNonQuery(CmdInsert);

                    }
                    catch (Exception e)
                    {
                        throw e;
                    }

                    return bResultado;
                }

                /// <summary>
                /// insertar datos - cabecera de Conteo Diario 
                /// </summary>
                /// <returns></returns>
                public Boolean Insertar_Cabecera_Conteo_Diario(String sArticuloID, String sReferencia, String sExistenciaSistema, String sTipoConteo
                                                                    , String sFechaConteoDiario, out String sConteoDiarioID, int iOrganizacionID = 1000230, int iClienteID = 1000000)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdCurrentNext = new OleDbCommand();
                    OleDbCommand CmdInsert = new OleDbCommand();
                    Boolean bResultado = true;
                    DataSet dsCurrentNext = new DataSet();
                    try
                    {
                        sConteoDiarioID = "";
                        //Selecciona el siguiente consecutivo para el id de conteo diario
                        int iCurrentNext = 0, iIncrement = 0;
                        CmdCurrentNext.CommandText = "SELECT CurrentNext , INCREMENTNO FROM AD_Sequence " +
                                                     " WHERE Name = 'XX_LO_CONTEO_DIARIO_ID' AND IsActive = 'Y' AND IsTableID = 'Y' AND IsAutoSequence = 'Y' ";

                        dsCurrentNext = Conexion.executeCmdDataSet(CmdCurrentNext);

                        foreach (DataRow r in dsCurrentNext.Tables[0].Rows)
                        {
                            iCurrentNext = Int32.Parse(r["CurrentNext"].ToString());
                            iIncrement = Int32.Parse(r["INCREMENTNO"].ToString());
                        }

                        //inserta el nuevo conteo diario
                        CmdInsert.CommandText =
                                " INSERT INTO XX_LO_CABECERA_CONTEO_DIARIO (AD_CLIENT_ID,AD_ORG_ID, CREATED ,CREATEDBY ,ISACTIVE ,UPDATED,  UPDATEDBY " +
                                "       ,  XX_LO_CONTEO_DIARIO_ID, M_PRODUCT_ID ,XX_LO_REFERENCIA, XX_LO_EXISTENCIA_SISTEMA , TIPO_CONTEO, XX_LO_EXISTENCIA_REAL, XX_LO_FECHA_CONTEO_DIARIO )  " +
                                "	VALUES( " + iClienteID + " , " + iOrganizacionID + " , CURRENT_TIMESTAMP , 0 , 'Y' , CURRENT_TIMESTAMP , 0 " +
                                "               , " + iCurrentNext + " , " + sArticuloID + " , '" + sReferencia + "' , " + sExistenciaSistema + " " +
                                "               , '" + sTipoConteo + "' , 0 , '" + sFechaConteoDiario + "' ) ";

                        bResultado = Conexion.executeCmdNonQuery(CmdInsert);

                        if (bResultado)
                        {
                            sConteoDiarioID = iCurrentNext.ToString();
                            //Aumenta el secuencial del conteo diario y lo actualiza
                            iCurrentNext += iIncrement;
                            CmdCurrentNext.CommandText = " UPDATE AD_Sequence " +
                                                         "   SET CurrentNext = " + iCurrentNext + " , Updated = CURRENT_TIMESTAMP " +
                                                         " WHERE Name = 'XX_LO_CONTEO_DIARIO_ID'";
                            bResultado = Conexion.executeCmdNonQuery(CmdCurrentNext);
                        }
                    }
                    catch (Exception e)
                    {
                        throw e;
                    }

                    return bResultado;
                }

                /// <summary>
                /// actualizar datos - ubicaciones vacias - Conteo Diario 
                /// </summary>
                /// <returns></returns>
                public Boolean Actualizar_Ubicaciones_Vacias_Conteo_Diario(String sConteoDiarioID, Int32 iCantidadUbicaciones)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdUpdate = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {
                        String sFechaActualizacion = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                        CmdUpdate.CommandText =
                                " UPDATE XX_LO_CABECERA_CONTEO_DIARIO " +
                                "	SET XX_LO_EXISTENCIA_SISTEMA = " + iCantidadUbicaciones + "  " +
                                "		, UPDATED = '" + sFechaActualizacion + "' " +
                                "	WHERE XX_LO_CONTEO_DIARIO_ID = " + sConteoDiarioID + " ";

                        bResultado = Conexion.executeCmdNonQuery(CmdUpdate);

                    }
                    catch (Exception e)
                    {
                        //bResultado = false;
                        throw e;
                    }

                    return bResultado;
                }

                /// <summary>
                /// actualizar datos - Conteo Diario 
                /// </summary>
                /// <returns></returns>
                public Boolean Actualizar_Articulo_Conteo_Diario(String sConteoDiarioID, int iOrganizacionID = 1000230, int iClienteID = 1000000)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdUpdate = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {
                        String sFechaActualizacion = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.ffffff");
                        //sFechaConteo ="2017-03-01 00:00:00";
                        CmdUpdate.CommandText =
                                " UPDATE XX_LO_CABECERA_CONTEO_DIARIO " +
                                "	SET XX_LO_EXISTENCIA_SISTEMA = ( SELECT SUM( XX_LO_QTY_ONPALLET ) FROM XX_LO_DETALLE_CONTEO_DIARIO WHERE XX_LO_CONTEO_DIARIO_ID = " + sConteoDiarioID + " GROUP BY XX_LO_CONTEO_DIARIO_ID ) " +
                                "		, UPDATED = '" + sFechaActualizacion + "' " +
                                "	WHERE XX_LO_CONTEO_DIARIO_ID = " + sConteoDiarioID + " " +
                                "	    AND  AD_CLIENT_ID = " + iClienteID + " " +
                                "	    AND  AD_ORG_ID = " + iOrganizacionID + " ";

                        bResultado = Conexion.executeCmdNonQuery(CmdUpdate);

                    }
                    catch (Exception e)
                    {
                        //bResultado = false;
                        throw e;
                    }

                    return bResultado;
                }

                /// <summary>
                /// eliminar datos - detalle de Conteo Diario 
                /// </summary>
                /// <returns></returns>
                public Boolean Eliminar_Articulo_Conteo_Diario(String sConteoDiarioID, int iOrganizacionID = 1000230, int iClienteID = 1000000)
                {
                    Conexion = new AccesoDatos.clsConexionOleDB(this.sCadenaConexion);
                    OleDbCommand CmdDelete = new OleDbCommand();
                    Boolean bResultado = true;
                    try
                    {
                        CmdDelete.CommandText =
                                " DELETE FROM XX_LO_DETALLE_CONTEO_DIARIO " +
                                "	WHERE XX_LO_CONTEO_DIARIO_ID = " + sConteoDiarioID + " " +
                                "	    AND  AD_CLIENT_ID = " + iClienteID + " " +
                                "	    AND  AD_ORG_ID = " + iOrganizacionID + " ";

                        bResultado = Conexion.executeCmdNonQuery(CmdDelete);

                    }
                    catch (Exception e)
                    {
                        throw e;
                    }

                    return bResultado;
                }

            #endregion

            #region Servicio

                /// <summary>
            /// Prueba la conexion a base de datos mediante un simple query
            /// </summary>
            /// <returns></returns>
                public Boolean probarConexionDB2(ref String sResultMessage)
            {
                Conexion = new AccesoDatos.clsConexionOleDB(this.CadenaConexion);
                OleDbCommand CmdSelect = new OleDbCommand();
                Boolean bResultado = false;

                try
                {

                    ////------------------------------------------------------------------------------
                    ////Ejecuta un select para probar si existe conexion
                    //CmdSelect.CommandText = "SELECT '1' FROM SYSIBM.SYSDUMMY1";
                    //if (String.Compare(Conexion.executeCmdScalar(CmdSelect), "1") == 0) {
                    //    bResultado = true;
                    //}

                    bResultado = Conexion.testConnection(ref sResultMessage);
                }
                catch (Exception e)
                {
                    throw e;
                }
                return bResultado;
            }

            #endregion

        #endregion

    }
}
