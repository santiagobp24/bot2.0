from bot import CapitalBot, EPIC, BASE_URL

def main():
    bot = CapitalBot()

    # Verificar que estamos usando la API DEMO.
    if "demo-api-capital.backend-capital.com" not in BASE_URL:
        raise RuntimeError(
            "La API no es DEMO. Se cancela por seguridad."
        )

    data = bot.request("GET", "/api/v1/positions")
    positions = data.get("positions", [])

    matching = [
        p for p in positions
        if p.get("market", {}).get("epic") == EPIC
    ]

    if not matching:
        print(f"No hay posiciones abiertas en {EPIC}.")
        return

    if len(matching) > 1:
        print("Hay varias posiciones en US100. No se cerrará ninguna.")
        for p in matching:
            print(p)
        return

    item = matching[0]
    deal_id = item.get("position", {}).get("dealId")
    direction = item.get("position", {}).get("direction")
    size = item.get("position", {}).get("size")

    if not deal_id:
        print("No se encontró el dealId. No se envió ninguna orden.")
        print(item)
        return

    print("\nPOSICIÓN ENCONTRADA")
    print(f"Mercado: {EPIC}")
    print(f"Dirección: {direction}")
    print(f"Tamaño: {size}")
    print(f"Deal ID: {deal_id}")

    confirm = input(
        "\nPara confirmar el cierre, escribe CERRAR: "
    ).strip()

    if confirm != "CERRAR":
        print("Cancelado. No se envió ninguna orden.")
        return

    result = bot.request(
        "DELETE", f"/api/v1/positions/{deal_id}"
    )

    print("Respuesta de solicitud de cierre:", result)

    reference = result.get("dealReference")
    if reference:
        confirmation = bot.request(
            "GET", f"/api/v1/confirms/{reference}"
        )
        print("Confirmación de Capital.com:", confirmation)

    remaining = bot.request("GET", "/api/v1/positions")
    still_open = [
        p for p in remaining.get("positions", [])
        if p.get("market", {}).get("epic") == EPIC
    ]

    if not still_open:
        print("Verificación: no quedan posiciones abiertas en US100.")
    else:
        print("ATENCIÓN: todavía aparece una posición abierta en US100.")
        print(still_open)


if __name__ == "__main__":
    main()
