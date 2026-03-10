import argparse
import dotenv
import os
import boto3

from tqdm import tqdm

dotenv.load_dotenv()
AWS_Key  = os.getenv('AWS_Key')
AWS_Secret_Key = os.getenv('AWS_Secret_Key')


class Sender:
    """
    Classe responsável por enviar arquivos para um bucket S3 da AWS.

    A classe cria uma conexão com o serviço S3 utilizando as credenciais
    armazenadas em variáveis de ambiente e permite realizar o upload
    de arquivos para um caminho específico dentro do bucket.
    """

    def __init__(self, bucket_name, bucket_folder):
        """
        Inicializa a classe Sender.

        Args:
            bucket_name (str): Nome do bucket S3 onde os arquivos serão enviados.
            bucket_folder (str): Caminho/pasta dentro do bucket onde os arquivos serão armazenados.
        """
        self.bucket_name = bucket_name
        self.bucket_folder = bucket_folder
        self.s3 = boto3.client("s3", 
                        aws_access_key_id=AWS_Key, 
                        aws_secret_access_key=AWS_Secret_Key,
                        region_name='sa-east-1',)   

    def process_file(self, filename):
        """
        Realiza o upload de um arquivo específico para o bucket S3.

        Após o upload ser concluído com sucesso, o arquivo local é removido.

        Args:
            filename (str): Caminho completo do arquivo a ser enviado.

        Returns:
            bool: True se o upload ocorrer com sucesso, False se ocorrer erro.
        """
        
        file = filename.split("/")[-1]
        bucket_path = os.path.join(self.bucket_folder, file)
        try:
            self.s3.upload_file(filename, 
                                self.bucket_name, 
                                bucket_path)
       
        except Exception as err:
            print(f"Error occurred: {err}")
            return False
        
        os.remove(filename)
        return True
    

    def process_file(self, folder):
     """
         Percorre uma pasta local procurando arquivos no formato .parquet
        e envia cada um deles para o bucket S3.

        Args:
            folder (str): Caminho da pasta que contém os arquivos .parquet.
        """
    files = [i for i in os.listdir(folder) if i.endswith(".parquet")]
    for f in tqdm(files):
        self.process_file(os.path.join(folder, f))
        
 
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", type=str)
    parser.add_argument("--bucket_path", default="f1/results", type=str)
    parser.add_argument("--folder", default="data", type=str)
    args = parser.parse_args()

    if args.bucket:
        send = Sender(args.bucket, args.bucket_path)
        send.process_folder(args.folder)

    else:
        print("sem bucket definido")