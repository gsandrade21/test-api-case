import boto3
import json
import datetime

s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("test_db")

def lambda_handler(event, context):

    #id counter
    id = 0

    #get raw input
    return_data = json.loads(event['body'])
    bucket_name = return_data['bucket_name']
    s3_file_name = return_data['object_key']

    #Read S3 file
    resp = s3_client.get_object(Bucket=bucket_name,Key=s3_file_name)
    data = resp['Body'].read().decode("utf-8") 
    data = data.split("\n")

    for each_data in data:
        
        split_data = each_data.split(",")

        try:
            #fix cpf/cnpj and date format
            cpf_cnpj = split_data[7]
            issue_date = split_data[30]
            issue_date = datetime.datetime.strptime(issue_date, '%d/%m/%Y').strftime('%y-%m-%d')
            expiration_date = split_data[31]
            expiration_date  = datetime.datetime.strptime(expiration_date , '%d/%m/%Y').strftime('%y-%m-%d')
            mask_simbols = '.-/'
            
            #remove mask from cpf/cnpj
            for i in mask_simbols:
                cpf_cnpj = cpf_cnpj.replace(i,"")               

            input = {
                'id': str(id),
                'ORIGINADOR': split_data[0],
                'DOC_ORIGINADOR': split_data[1],
                'CEDENTE': split_data[2],
                'DOC_CEDENTE': split_data[3],
                'DOC_CEDENTE': split_data[4],
                'CCB':split_data[5],
                'CLIENTE': split_data[6],
                'CPF_CNPJ': cpf_cnpj,
                'ENDERECO': split_data[8],
                'CEP': split_data[9],
                'CIDADE': split_data[10],
                'UF': split_data[11],
                'VALOR_DO_EMPRESTIMO': split_data[12],
                'VALOR_PARCELA': split_data[13],
                'TOTAL_PARCELAS': split_data[14],
                'PARCELA': split_data[15],
                'DATA_DE_EMISSAO': issue_date,
                'DATA_DE_VENCIMENTO': expiration_date,
                'PRECO_DE_AQUISICAO': split_data[18]
            }

            #add to DynamoDB
            store = table.put_item(Item=input)
            id+=1
            
            resp = json.dumps('Data was added successfully!')
            
        except Exception as e:
            resp = json.dumps('Data was not added!')
            
    return {
        'statusCode': 200,
        'body': resp
    }
