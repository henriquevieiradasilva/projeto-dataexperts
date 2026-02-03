import uuid
from datetime import datetime
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType,
    LongType, DateType
)

LOG_SCHEMA = StructType([
    StructField("log_id", StringType(), False),
    StructField("camada", StringType(), False),
    StructField("job_nome", StringType(), False),
    StructField("acao", StringType(), False),
    StructField("nome_objeto", StringType(), True),
    StructField("fonte", StringType(), True),
    StructField("path_origem", StringType(), True),
    StructField("path_destino", StringType(), True),
    StructField("status", StringType(), False),
    StructField("mensagem_erro", StringType(), True),
    StructField("data_inicio", TimestampType(), False),
    StructField("data_fim", TimestampType(), True),
    StructField("duracao_ms", LongType(), True),
    StructField("registros_lidos", LongType(), True),
    StructField("registros_gravados", LongType(), True),
    StructField("data_execucao", DateType(), False),
])

def create_pre_log(
    camada,
    tabela_log,
    job_nome,
    acao,
    nome_objeto=None,
    fonte=None,
    path_origem=None,
    path_destino=None
):
    return {
        "log_id": str(uuid.uuid4()),
        "tabela_log": tabela_log,  # 👈 FUNDAMENTAL
        "camada": camada,
        "job_nome": job_nome,
        "acao": acao,
        "nome_objeto": nome_objeto,
        "fonte": fonte,
        "path_origem": path_origem,
        "path_destino": path_destino,
        "status": "RUNNING",
        "mensagem_erro": None,
        "data_inicio": datetime.now(),
        "data_fim": None,
        "duracao_ms": None,
        "registros_lidos": None,
        "registros_gravados": None,
        "data_execucao": datetime.now().date(),
    }


def finalize_log(
    spark,
    log,
    status="SUCCESS",
    mensagem_erro=None,
    registros_lidos=None,
    registros_gravados=None
):
    fim = datetime.now()

    log["data_fim"] = fim
    log["duracao_ms"] = int(
        (fim - log["data_inicio"]).total_seconds() * 1000
    )
    log["status"] = status
    log["mensagem_erro"] = mensagem_erro
    log["registros_lidos"] = registros_lidos
    log["registros_gravados"] = registros_gravados

    tabela_log = log["tabela_log"]  # 👈 NÃO remove

    log_df = {k: v for k, v in log.items() if k != "tabela_log"}

    df = spark.createDataFrame([log_df], schema=LOG_SCHEMA)

    df.write.mode("append").saveAsTable(tabela_log)


