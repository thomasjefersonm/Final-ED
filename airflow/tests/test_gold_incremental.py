"""Testes de integração Spark/Delta. Execute no ambiente Spark do Airflow."""

import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "include"))

if os.getenv("RUN_SPARK_TESTS") == "1":
    from pipeline.gold import _gravar_dimensao, _gravar_fato, _selecionar_incremento
    from pipeline.spark import criar_spark


@unittest.skipUnless(os.getenv("RUN_SPARK_TESTS") == "1", "requer Spark e Delta Lake")
class TestGoldIncremental(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        cls.spark = criar_spark("test_gold_incremental")
        cls.addClassCleanup(cls.spark.stop)
        cls.spark.conf.set("spark.sql.shuffle.partitions", "2")
        cls.spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")

    def test_dimensao_preserva_historico_ao_reexecutar(self):
        with TemporaryDirectory() as caminho:
            inicial = self.spark.createDataFrame([(1, "Nome anterior")], "cd_cargo long, ds_cargo string")
            alterada = self.spark.createDataFrame(
                [(1, "Nome atualizado"), (2, "Novo cargo")], inicial.schema
            )
            datas = [datetime(2026, 1, dia, tzinfo=timezone.utc) for dia in (1, 2, 3)]
            _gravar_dimensao(self.spark, inicial, caminho, "cargo", datas[0])
            _gravar_dimensao(self.spark, alterada, caminho, "cargo", datas[1])
            _gravar_dimensao(self.spark, alterada, caminho, "cargo", datas[2])
            linhas = self.spark.read.format("delta").load(caminho).collect()

            self.assertEqual(len(linhas), 3)
            anterior = next(linha for linha in linhas if not linha.registro_atual)
            atual = next(linha for linha in linhas if linha.cd_cargo == 1 and linha.registro_atual)
            self.assertEqual(anterior.ds_cargo, "Nome anterior")
            self.assertEqual(atual.ds_cargo, "Nome atualizado")
            self.assertEqual(anterior.vigencia_fim, atual.vigencia_inicio)
            self.assertNotEqual(anterior.sk_cargo, atual.sk_cargo)
            self.assertEqual(sum(linha.registro_atual for linha in linhas), 2)

    def test_fato_seleciona_novos_e_correcoes_sem_duplicar(self):
        with TemporaryDirectory() as caminho:
            chaves = ("id_turno", "id_secao", "sq_candidato")
            medidas = ("qt_votos",)
            schema = (
                "id_turno long, id_secao long, sq_candidato long, id_local_votacao long, "
                "qt_votos int, dt_carga timestamp, sk_secao string"
            )
            dia = datetime(2026, 1, 2, tzinfo=timezone.utc)
            anterior = datetime(2026, 1, 1, tzinfo=timezone.utc)
            inicial = self.spark.createDataFrame(
                [(1, 1, 1, 10, 5, dia, "secao-original"), (1, 2, 1, 20, 6, dia, "secao-2"),
                 (1, 4, 1, 40, 7, dia, "secao-4")],
                schema,
            )
            origem = self.spark.createDataFrame(
                [(1, 1, 1, 11, 8, dia, "secao-corrigida"), (1, 2, 1, 20, 6, dia, "secao-2"),
                 (1, 3, 1, 30, 9, anterior, "secao-3"), (1, 4, 1, 40, 99, anterior, "secao-4")],
                schema,
            )
            _gravar_fato(self.spark, inicial, caminho, chaves, medidas)
            incremento = _selecionar_incremento(self.spark, origem, caminho, chaves, medidas)
            self.assertEqual({linha.id_secao for linha in incremento.collect()}, {1, 3})
            _gravar_fato(self.spark, incremento, caminho, chaves, medidas)
            self.assertEqual(
                _selecionar_incremento(self.spark, origem, caminho, chaves, medidas).take(1), []
            )
            linhas = self.spark.read.format("delta").load(caminho).collect()
            self.assertEqual(len(linhas), 4)
            self.assertEqual({linha.id_secao: linha.qt_votos for linha in linhas}, {1: 8, 2: 6, 3: 9, 4: 7})
            self.assertEqual(next(linha.sk_secao for linha in linhas if linha.id_secao == 1), "secao-corrigida")


if __name__ == "__main__":
    unittest.main()
