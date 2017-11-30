using System;
using System.Web.Mail;
using System.Collections.Specialized;
using System.IO;
using System.Data;
using System.Threading;
using System.Collections;


public class Email
{
    public static string strCodError1 = String.Empty;

	#region Clases	
	//public static GenerarArchivosLog objTextFile = new GenerarArchivosLog();	
	#endregion
    
	public Email()
    {
        //
        // TODO: Add constructor logic here
        //

    }

    public bool EnviarEmail(string De, string Para, string Copia, string Asunto, System.Web.Mail.MailFormat Formato, string CuerpoEmail, string ServidorSMTP)
    {
        bool auxiliar = false;
        int compAdjuntar;
		//int compPara;
		

        MailMessage mail = new MailMessage();

        mail.From = De;
        mail.To = Para;
		mail.Cc = Copia;
        mail.Subject = Asunto;
        mail.BodyFormat = Formato;

       
        mail.Body = CuerpoEmail;
        SmtpMail.SmtpServer.Insert(0, ServidorSMTP);
        try
        {
            SmtpMail.Send(mail);
            auxiliar = true;
        }
        catch (Exception ex)
        {
            //LOG
            auxiliar = false;
			strCodError1 = ex.Message;								
			//objTextFile.EscribirLog("  ---> ERROR -- ENVIANDO EMAIL. CODIGO: " + strCodError1);
        }

        return auxiliar;
    }
}
