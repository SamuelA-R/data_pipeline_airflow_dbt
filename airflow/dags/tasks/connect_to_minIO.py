import boto3
import os

class MinIOConnect:
    def __init__(self, endpoint_url, access_key, secret_key):
        self.s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )

    def connect(self):
        bucket_name = ["bronze"]
        for name in bucket_name:
            try:
                self.s3.head_bucket(Bucket=name)
                print(f"Bucket '{name}' já existe.")
            except self.s3.exceptions.ClientError:
                self.s3.create_bucket(Bucket=name)
                print(f"Bucket '{name}' criado com sucesso.")

    def upload_file(self, bucket_name, base_path):
        arquivos = [f for f in os.listdir(base_path) if os.path.isfile(os.path.join(base_path, f))]
        if not arquivos:
            print(f"Nenhum arquivo em {base_path}.")
            return

        for f in arquivos:
            full_path = os.path.join(base_path, f)
            try:
                self.s3.upload_file(full_path, bucket_name, f)
                print(f"✔ {f} → {bucket_name}")
            except Exception as e:
                print(f"❌ Erro ao enviar {f}: {e}")

    def connect_and_upload_files_to_minio(self, bucket_name, base_path):
        self.connect()
        self.upload_file(bucket_name=bucket_name, base_path=base_path)
        return "Upload concluído"