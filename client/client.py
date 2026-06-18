import os
import time
import random
import requests

CLIENT_ID = os.getenv("CLIENT_ID")
TARGET_PEER = os.getenv("TARGET_PEER")

TOTAL_REQUESTS = random.randint(10, 50)


def main():
    print(f"[{CLIENT_ID}] Iniciado")
    print(f"[{CLIENT_ID}] Enviará {TOTAL_REQUESTS} pedidos para {TARGET_PEER}")

    time.sleep(3)

    for i in range(1, TOTAL_REQUESTS + 1):
        request_id = f"{CLIENT_ID}-{i}"

        print(f"[{CLIENT_ID}] Solicitando acesso ao recurso R | pedido {request_id}")

        try:
            response = requests.post(
                f"{TARGET_PEER}/request_resource",
                json={
                    "client_id": CLIENT_ID,
                    "request_id": request_id
                },
                timeout=60
            )

            data = response.json()

            if data["status"] == "COMMITTED":
                print(f"[{CLIENT_ID}] COMMITTED recebido do {data['peer']} | pedido {request_id}")

        except Exception as e:
            print(f"[{CLIENT_ID}] Erro no pedido {request_id}: {e}")

        sleep_time = random.randint(1, 5)
        print(f"[{CLIENT_ID}] Aguardando {sleep_time}s antes do próximo pedido")
        time.sleep(sleep_time)

    print(f"[{CLIENT_ID}] Finalizou todos os pedidos")


if __name__ == "__main__":
    main()