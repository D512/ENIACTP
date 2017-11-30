using System;
//using System.Web.Mail;
using System.Collections.Specialized;
using System.IO;
using System.Data;
using System.Threading;
using System.Collections;
using System.Net;
using System.Net.Mail;

namespace ReporteOC
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




            string to = Para;
            string from = De;
            MailMessage message = new MailMessage(from, to);
            message.Subject = Asunto;
            if (Copia != "")
            {
                message.CC.Add(Copia);
            }
            message.Body = CuerpoEmail;
            message.BodyEncoding = System.Text.Encoding.UTF8;
            message.IsBodyHtml = true;

            //SmtpClient client = new SmtpClient("smtp.gmail.com");

            SmtpClient client = new SmtpClient(Class1.strServer);


            //client.EnableSsl = true;

            client.UseDefaultCredentials = true;

            //client.Credentials = new System.Net.NetworkCredential("", "");


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


            return auxiliar;
        }

    }

}