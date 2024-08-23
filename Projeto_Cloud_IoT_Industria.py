from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import random
import boto3
import json

#Coleta de Dados com IoT usando AWS IoT Core

# Configurações do AWS IoT Core
client_id = "padaria_sensor_client"
endpoint = "your-endpoint.iot.us-east-1.amazonaws.com"  # Substitua pelo seu endpoint do AWS IoT Core
root_ca = "path/to/root-CA.crt"  # Substitua pelo caminho do certificado root CA
private_key = "path/to/private.pem.key"  # Substitua pelo caminho da chave privada do dispositivo
certificate = "path/to/certificate.pem.crt"  # Substitua pelo caminho do certificado do dispositivo
topic = "padaria/sensores"

# Configurações do cliente MQTT para AWS IoT
client = AWSIoTMQTTClient(client_id)
client.configureEndpoint(endpoint, 8883)
client.configureCredentials(root_ca, private_key, certificate)

# Função de callback para conexão
def on_connect(client, userdata, flags, rc):
    print("Conectado ao AWS IoT Core")

# Função para publicar dados
def publish_data(client, topic, data):
    client.publish(topic, data, 1)

# Conectando ao AWS IoT Core
client.connect()

# Loop de publicação de dados
try:
    while True:
        # Simulando leituras de sensores
        temperatura = round(random.uniform(20.0, 30.0), 2)
        umidade = round(random.uniform(40.0, 60.0), 2)
        
        # Publicando dados no MQTT
        data = f"temperatura:{temperatura},umidade:{umidade}"
        publish_data(client, topic, data)
        
        print(f"Dado publicado: {data}")
        time.sleep(10)  # Atraso entre as publicações

except KeyboardInterrupt:
    print("Interrompido pelo usuário")
finally:
    client.disconnect()



#Armazenamento de Dados na Nuvem usando AWS DynamoDB e AWS Lambda

# Conectando ao DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('PadariaSensores')

def lambda_handler(event, context):
    # Extrair dados do evento MQTT
    data = json.loads(event['Records'][0]['Sns']['Message'])
    temperatura = float(data['temperatura'].split(':')[1])
    umidade = float(data['umidade'].split(':')[1])
    
    # Armazenar dados no DynamoDB
    response = table.put_item(
        Item={
            'id': context.aws_request_id,
            'temperatura': temperatura,
            'umidade': umidade,
            'timestamp': int(time.time())
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Dados armazenados no DynamoDB')
    }




#Monitoramento e Notificações usando AWS Lambda e Amazon SNS

# Conectando ao DynamoDB e SNS
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')
table = dynamodb.Table('PadariaSensores')
sns_topic_arn = 'arn:aws:sns:us-east-1:123456789012:TemperaturaAlerta'  # Substitua pelo ARN do seu tópico SNS

def lambda_handler(event, context):
    # Obtendo o último registro
    response = table.scan(Limit=1)
    item = response['Items'][0]
    temperatura = item['temperatura']
    
    # Verificando se a temperatura excede o limite
    if temperatura > 25.0:
        message = f"Alerta: A temperatura atual é {temperatura}°C"
        sns.publish(TopicArn=sns_topic_arn, Message=message)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Monitoramento concluído')
    }




#Análise e Visualização de Dados com AWS QuickSight

# Conectando ao serviço QuickSight
client = boto3.client('quicksight')

# Parâmetros para criar o dataset (substitua os valores pelos correspondentes da sua conta)
aws_account_id = '123456789012'
dataset_name = 'PadariaSensoresDataset'
dynamodb_arn = 'arn:aws:dynamodb:us-east-1:123456789012:table/PadariaSensores'
dataset_arn = 'arn:aws:quicksight:us-east-1:123456789012:dataset/PadariaSensoresDataset'

# Criando um dataset a partir de uma tabela DynamoDB
response = client.create_data_set(
    AwsAccountId=aws_account_id,
    DataSetId=dataset_name,
    Name=dataset_name,
    PhysicalTableMap={
        'PadariaSensoresTable': {
            'RelationalTable': {
                'DataSourceArn': dynamodb_arn,
                'Schema': 'public',
                'Name': 'PadariaSensores',
                'InputColumns': [
                    {'Name': 'id', 'Type': 'STRING'},
                    {'Name': 'temperatura', 'Type': 'DECIMAL'},
                    {'Name': 'umidade', 'Type': 'DECIMAL'},
                    {'Name': 'timestamp', 'Type': 'INTEGER'},
                ]
            }
        }
    },
    ImportMode='SPICE'  # ou 'DIRECT_QUERY' para consulta direta
)

print(response)

# Criando uma análise básica (exemplo de automação, na prática, a criação de visualizações é feita no console)
analysis_id = 'PadariaAnalysis'
response = client.create_analysis(
    AwsAccountId=aws_account_id,
    AnalysisId=analysis_id,
    Name='Padaria Sensor Analysis',
    SourceEntity={
        'SourceTemplate': {
            'DataSetReferences': [
                {
                    'DataSetPlaceholder': 'PadariaSensoresDataset',
                    'DataSetArn': dataset_arn
                },
            ]
        }
    }
)

print(response)
