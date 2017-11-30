using System;
//using System.Web.Mail;
using System.Collections.Specialized;
using System.IO;
using System.Data;
using System.Threading;
using System.Collections;
using System.Net;
using System.Net.Mail;

namespace TP_FacturaFarmatodo
{

    public class Email
    {
        public static string strCodError1 = String.Empty;

        #region Clases
        public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();
        #endregion

        public Email()
        {
            //
            // TODO: Add constructor logic here
            //

        }


        public bool EnviarEmail(string De, string Para, string Copia, string Asunto, string CuerpoEmail, string ServidorSMTP)
        {
            bool auxiliar = false;
            int compAdjuntar;
            //int compPara;
            try
            {
                //string to = Para;
                //string from = De;
                //MailMessage message = new MailMessage(from, to);

                MailMessage message = new MailMessage();
                message.From = new MailAddress(De);
                message.Subject = Asunto;

                #region DESTINATARIOS DEL CORREO
                if (Para.IndexOf(";") > 0)
                {
                    string[] destinatarios = Para.Split(';');

                    foreach (string to in destinatarios)
                    {
                        if (!String.IsNullOrEmpty(to))
                        {
                            message.To.Add(new MailAddress(to));
                        }
                    }
                }
                else
                {
                    if (!String.IsNullOrEmpty(Para))
                    {
                        message.To.Add(new MailAddress(Para));
                    }
                }
                #endregion

                #region COPIAR CORREO A
                if (!String.IsNullOrEmpty(Copia))
                {
                    if (Copia.IndexOf(";") > 0)
                    {
                        string[] otrosdestinatarios = Copia.Split(';');

                        foreach (string cc in otrosdestinatarios)
                        {
                            if (!String.IsNullOrEmpty(cc))
                            {
                                message.CC.Add(new MailAddress(cc));
                            }
                        }
                    }
                    else
                    {
                        message.CC.Add(new MailAddress(Copia));
                    }
                }

                #endregion

                message.Body = CuerpoEmail;
                message.BodyEncoding = System.Text.Encoding.UTF8;
                message.IsBodyHtml = true;

                SmtpClient client = new SmtpClient(Class1.strServer);

                client.UseDefaultCredentials = true;

                try
                {
                    client.Send(message);
                    auxiliar = true;
                }

                catch (SmtpException ex)
                {
                    auxiliar = false;
                    strCodError1 = ex.Message;
                    objTextFile.EscribirLog("  ---> ERROR -- ENVIANDO EMAIL. CODIGO: " + strCodError1);
                }
                message.Dispose();

            }
            catch (Exception ex)
            {
                strCodError1 = ex.Message;
                objTextFile.EscribirLog("  ---> ERROR ENVIAR EMAIL. CODIGO: " + strCodError1 + "PARA:" + Para + ".DE:" + De);
            }
            return auxiliar;
        }

    }

}