using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AccesoDatos
{
    public class clsConexionOleDB
    {

        #region Variables

        private String sServidor;     //Nombre del Servidor de Base de Datos.
        private String sBase;         //Nombre de la Base de Datos.
        private String sUsuario;      //Nombre del Usuario de la Base de Datos.
        private String sClave;        //Password del Usuario de la Base de Datos.
        private String sCadenaConexion;               //Cadena de Conexion Completa.
        private OleDbConnection cnnConnectionAtomic;  //conexion utilizada para operaciones transaccionales.

        private static volatile clsConexionOleDB instance;
        private static readonly object padlock = new object();

        #endregion

        #region Propiedades

        /// <summary>
        /// Propiedad para el Nombre del servidor de base de datos (datasource)
        /// </summary>
        public String Servidor
        {
            get { return sServidor; }
            set { sServidor = value; }
        }

        /// <summary>
        /// Propiedad para el Nombre de la base de datos (Default Base)
        /// </summary>
        public String Base
        {
            get { return sBase; }
            set { sBase = value; }
        }

        /// <summary>
        /// Propiedad para el Usuario utilizado para realizar conexión a la base de datos
        /// </summary>
        public String Usuario
        {
            get { return sUsuario; }
            set { sUsuario = value; }
        }

        /// <summary>
        /// Propiedad para la clave de conexión al motor de datos
        /// </summary>
        public String Clave
        {
            get { return sClave; }
            set { sClave = value; }
        }

        /// <summary>
        /// Propiedad para la cadena de conexión utilizada para realizar
        /// la conexion. Por default, se maneja una cadena para DB2
        /// </summary>
        public String CadenaConexion
        {
            get
            {
                if (sCadenaConexion.Length == 0)
                {
                    if (Servidor.Length > 0 && Base.Length > 0 && Usuario.Length > 0)
                    {
                        StringBuilder sCadena = new StringBuilder("Provider=IBMDA400.1; Data Source=<SERVIDOR>; User ID=<USUARIO>; Password=<CLAVE>; Default Collection=<BASE>");
                        sCadena.Replace("<SERVIDOR>", Servidor);
                        sCadena.Replace("<USUARIO>", Usuario);
                        sCadena.Replace("<CLAVE>", Clave);
                        sCadena.Replace("<BASE>", Base);
                        sCadenaConexion = sCadena.ToString();
                    }
                    else
                    {
                        throw new System.Exception("No se pudo establecer la cadena de conexión.");
                    }
                }
                return sCadenaConexion;
            }
            set { sCadenaConexion = value; }
        }

        #endregion

        #region Constructores

        /// <summary>
        /// Constructor para inicializar la clase a partir de una cadena de conexion pre-formada
        /// </summary>
        /// <param name="pCadenaConexion">Cadena de conexion a utilizar</param>
        public clsConexionOleDB(String pCadenaConexion)
        {
            this.CadenaConexion = pCadenaConexion;
        }

        /// <summary>
        /// Contructor para inicializar la clase a partir de las datos de servidor, base, usuario, y password
        /// </summary>
        /// <param name="pServidor">>Nombre del servidor a utilizar</param>
        /// <param name="pBase">Nombre de la base de datos</param>
        /// <param name="pUsuario">Usuario de la base de datos</param>
        /// <param name="Password">Password del usuario de la base de datos</param>
        public clsConexionOleDB(String pServidor, String pBase, String pUsuario, String Password)
        {
            this.Servidor = pServidor;
            this.Base = pBase;
            this.Usuario = pUsuario;
            this.Clave = Password;
        }

        private clsConexionOleDB()
        {
            this.Servidor = "";
            this.Base = "";
            this.Usuario = "";
            this.Clave = "";
            this.CadenaConexion = "";
        }

        public void ConstruyeConexion()
        {
            if (Servidor.Length > 0 && Usuario.Length > 0)
            {
                StringBuilder sCadena = new StringBuilder("Provider=IBMDA400.1; Data Source=<SERVIDOR>; User ID=<USUARIO>; Password=<CLAVE>; Default Collection=<BASE>");
                sCadena.Replace("<SERVIDOR>", Servidor);
                sCadena.Replace("<USUARIO>", Usuario);
                sCadena.Replace("<CLAVE>", Clave);
                sCadena.Replace("<BASE>", "COMPIERE");
                sCadenaConexion = sCadena.ToString();
            }
        }

        /// <summary>
        /// Permite verificar la conexion a la base de datos
        /// </summary>
        /// <param name="sResultMessage">Mensaje de resultado de la prueba</param>
        /// <returns>True = Conexion exitosa, False = No fue posible establecer conexion</returns>
        public bool testConnection(ref String sResultMessage)
        {
            bool bResult = false;
            OleDbConnection myConnection = new OleDbConnection(this.CadenaConexion);

            try
            {
                myConnection.Open();
                bResult = true;
                sResultMessage = "Connection is Ok";
            }
            catch (Exception e)
            {
                sResultMessage = e.Message;
            }
            finally
            {
                myConnection.Close();
                myConnection.Dispose();
            }

            return bResult;
        }

        #endregion

        #region Metodos ExecuteNonQuery

        /// <summary>
        /// Ejecuta el Commando con el metodo ExecuteNonQuery.
        /// </summary>
        /// <param name="Command">Comando a ejecutar</param>
        /// <param name="iRowsAffected">Cantidad de filas afectadas por el comando</param>
        /// <returns>>True = Ejecucion exitosa, False = Ejecucion errorea</returns>
        public Boolean executeCmdNonQuery(OleDbCommand Command, Int32 iRowsAffected = 0)
        {
            Boolean bResult = false; //Para devolver la respuesta.
            OleDbConnection myConnection = new OleDbConnection(CadenaConexion);    //Para conectar la base de datos.

            try
            {
                //Abro la conexion a la Base de Datos.
                myConnection.Open();

                //Inicializo los valores del Command.
                Command.Connection = myConnection;
                Command.CommandType = CommandType.Text;

                //Ejecuto el Comando.
                iRowsAffected = Command.ExecuteNonQuery();
                bResult = true;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                //Cierro la conexion a la base de datos
                myConnection.Close();
                myConnection.Dispose();
            }

            return bResult;
        }

        /// <summary>
        /// Ejecuta un NonQuery para un OleDBCommand que ya tiene una transaccion asociada
        /// </summary>
        /// <param name="Command">OleDBCommand a ejecutar</param>
        /// <param name="iRowsAffected">Cantidad de filas afectadas por el comando</param>
        /// <returns>True = Ejecucion exitosa, False = Ejecucion errorea</returns>
        public Boolean executeCmdNonQueryAtomico(OleDbCommand Command, Int32 iRowsAffected = 0)
        {
            Boolean bResult = false; //Para devolver la respuesta.

            try
            {
                //Asigna la conexion que contiene la transaccion
                Command.Connection = Command.Transaction.Connection;
                Command.CommandType = CommandType.Text;

                //Ejecuto el Comando.
                iRowsAffected = Command.ExecuteNonQuery();
                bResult = true;
            }
            catch (Exception e)
            {
                throw e;
            }

            return bResult;
        }

        public static clsConexionOleDB getInstance()
        {
            if (instance == null)
            {
                lock (padlock)
                {
                    if (instance == null)
                    {
                        instance = new clsConexionOleDB();
                    }
                }
            }
            return instance;
        }

        #endregion

        #region Metodos ExecuteScalar

        /// <summary>
        /// Ejecuta el Commando con el metodo ExecuteScalar.
        /// </summary>
        /// <param name="Command">Comando a ejecutar contra la Base de Datos.</param>
        /// <returns>Resultado de la ejeccucion del comando.</returns>
        public String executeCmdScalar(OleDbCommand Command)
        {
            OleDbConnection myConnection = new OleDbConnection(CadenaConexion); //Para conectar la base de datos.
            String sResult = ""; //Para devolver la respuesta.

            try
            {
                //Abro la conexion a la Base de Datos.
                myConnection.Open();

                //Inicializo los valores del Command.
                Command.Connection = myConnection;
                Command.CommandType = CommandType.Text;

                //Ejecuto el Comando.
                Object resultado = Command.ExecuteScalar();

                if ((!object.ReferenceEquals(resultado, System.DBNull.Value)) && (resultado != null))
                {
                    sResult = resultado.ToString();
                }
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                //Cierro la conexion a la base de datos
                myConnection.Close();
                myConnection.Dispose();
            }
            return sResult;
        }

        /// <summary>
        /// Ejecuta un ExecuteScalar para un OleDBCommand asociado a una transaccion
        /// </summary>
        /// <param name="Command">OleDBCommand con su transaccion asociada</param>
        /// <returns>El escalar obtenido</returns>
        public String executeCmdScalarAtomico(OleDbCommand Command)
        {
            String sResult = String.Empty; //Para devolver la respuesta.

            try
            {
                //Inicializo los valores del Command.
                Command.Connection = Command.Transaction.Connection;
                Command.CommandType = CommandType.Text;

                //Ejecuto el Comando.
                Object resultado = Command.ExecuteScalar();

                if ((!object.ReferenceEquals(resultado, System.DBNull.Value)) && (resultado != null))
                {
                    sResult = (String)resultado;
                }
            }
            catch (Exception e)
            {
                throw e;
            }

            return sResult;
        }

        #endregion

        #region Metodos Dataset

        /// <summary>
        /// Ejecuta el Commando y devuelve un Dataset.
        /// </summary>
        /// <param name="Command">Comando a ejecutar contra la Base de Datos.</param>
        /// <returns>Un dataset con el resultado de la ejecucion del comando.</returns>
        public DataSet executeCmdDataSet(OleDbCommand Command)
        {
            OleDbConnection myConnection = new OleDbConnection(CadenaConexion); //Para conectar la base de datos.
            OleDbDataAdapter myOleDbDataAdapter;  //Para rellenar el dataset.
            DataSet dsResult = null;  //Para devolver la respuesta.

            try
            {

                //Abro la conexion a la base de datos.
                myConnection.Open();

                //Llamo al CommandText pasado como parametro.
                Command.Connection = myConnection;
                Command.CommandType = CommandType.Text;

                //Ejecuto el CommandText y relleno el Dataset.
                DataSet myDataSet = new DataSet();
                myOleDbDataAdapter = new OleDbDataAdapter(Command);

                myOleDbDataAdapter.Fill(myDataSet);
                dsResult = myDataSet;
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                //Cierro la conexion a la base de datos.
                myConnection.Close();
                myConnection.Dispose();
            }

            return dsResult;
        }

        /// <summary>
        /// Ejecuta un comando y dsevuelve un dataset
        /// </summary>
        /// <param name="Command">Comando a ejecutar</param>
        /// <returns>Un dataset con el contenido</returns>
        public DataSet executeCmdDataSetAtomico(OleDbCommand Command)
        {
            OleDbDataAdapter myOleDbDataAdapter; //Para rellenar el dataset.
            DataSet dsResult = null; //Para devolver la respuesta.

            try
            {
                Command.Connection = Command.Transaction.Connection;
                Command.CommandType = CommandType.Text;

                //Ejecuto el CommandText y relleno el Dataset.
                DataSet myDataSet = new DataSet();
                myOleDbDataAdapter = new OleDbDataAdapter(Command);
                myOleDbDataAdapter.Fill(myDataSet);
                dsResult = myDataSet;
            }
            catch (Exception e)
            {
                throw e;
            }

            return dsResult;
        }

        #endregion

        #region Atomicidad

        /// <summary>
        /// Inicia una transaccion con su respectiva conexion, dicha conexion sera cerrada
        /// al momento de commit o rollback
        /// </summary>
        /// <returns>El objeto transaccion que se inició</returns>
        public OleDbTransaction Begin_Transaccion()
        {
            OleDbTransaction CommandTrn; //Para ejecutar el CommandText en una transacción.

            try
            {
                //Instancio una nueva conexion 
                cnnConnectionAtomic = new OleDbConnection(CadenaConexion);
                cnnConnectionAtomic.Open(); //Abrir conexion a BD
                CommandTrn = cnnConnectionAtomic.BeginTransaction(); //Iniciar transaccion
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return CommandTrn;
        }

        /// <summary>
        ///  Hace commit al objeto transaccion que recibe, y cierra la conexion utilizada
        /// </summary>
        /// <param name="CommandTrn">Objeto transaccion que necesita commit</param>
        public void Commit_Transaccion(OleDbTransaction CommandTrn)
        {
            try
            {
                CommandTrn.Commit();
                this.cnnConnectionAtomic.Close();
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        /// <summary>
        /// Hace rollback al objeto transaccion que recibe, y cierra la conexion utilizada
        /// </summary>
        /// <param name="CommandTrn">Objeto transaccion al que se le hara rollback</param>
        public void Rollback_Transaccion(OleDbTransaction CommandTrn)
        {
            try
            {
                CommandTrn.Rollback();
                this.cnnConnectionAtomic.Close();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion
    }
}
