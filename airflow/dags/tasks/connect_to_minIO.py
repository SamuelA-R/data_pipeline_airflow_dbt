import boto3
import os

class MinIOConnect():
    def __init__(self, endpoint_url, access_key, secret_key):
        self.s3 = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def connect(self):
        #parametros de conexão ao MinIO
        s3 = self.s3
        bucket_names = ['bronze', 'silver', 'gold']

        # Cria o bucket se não existir
        try:
            for bucket_name in bucket_names:
                s3.head_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' já existe.")
        except s3.exceptions.ClientError:
            for bucket_name in bucket_names:
                s3.create_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' criado com sucesso.")


    def upload_file(self, list_file_name, bucket_name, base_path, object_name=None, date_today=None):
        #caminho completo
        full_path = os.path.join(base_path, file_name)

        for file_name in list_file_name:
            if object_name is None:
                object_name = file_name + f"-{date_today}.json"
                
            try:
                self.s3.upload_file(full_path, bucket_name, object_name)
                print(f"Arquivo '{file_name}' enviado para o bucket '{bucket_name}' como '{object_name}'.")

            except Exception as e:
                print(f"Erro ao enviar o arquivo: {e}")
    
def connect_and_upload_files_to_minio(self, bucket_name, list_file_name, list_file_name2=None, base_path=None, object_name=None, date_today=None):
    # Garante que os buckets existam
    self.connect()
    # Chama upload_file com argumentos nomeados (boa prática)
    self.upload_file(
        list_file_name=list_file_name,
        bucket_name=f"{bucket_name}_{date_today}",
        base_path=base_path,
        object_name=object_name
    )
    return "Upload concluído"

