import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders


def adjuntar_archivo(filename):
    with open(filename, 'rb') as file:
        file_part = MIMEBase('application', 'octet-stream')
        file_part.set_payload((file).read())
        encoders.encode_base64(file_part)
        file_part.add_header('Content-Disposition',
                             f"attachment; filename={filename}")
        return file_part


def enviar_mail_con_adjuntos(user_sender, password,
                             user_target, asunto, body, archivos=[]):
    msg_multiparte = MIMEMultipart()

    # detalles de envio
    msg_multiparte['From'] = user_sender
    msg_multiparte['To'] = user_target
    msg_multiparte['Subject'] = asunto
    # Cuerpo del mesnaje.
    # El texto contenido. Convierte el string a texto plano formateado mail
    msg_multiparte.attach(MIMEText(body, 'plain'))

    # EDfino funcio que uso mas de una vez
    # abriendo el archivo

    for name in archivos:
        archivo_adjunto = adjuntar_archivo(name)
        msg_multiparte.attach(archivo_adjunto)

    # Pasa el mensaje a string
    msg = msg_multiparte.as_string()

    # inicia conexion con el servidor
    server = smtplib.SMTP('smtp.gmail.com', 587)
    # establece conexion segura
    server.starttls()

    # Se logea en el usuario que envia
    server.login(user_sender, password)
    # envia el mail
    server.sendmail(user_sender, user_target, msg)

    server.quit()
