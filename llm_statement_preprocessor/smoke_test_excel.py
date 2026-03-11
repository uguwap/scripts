from pathlib import Path

from excel_builder import build_excel_file


def main() -> None:
    header = {
        "debtor_account_number": "40702810600030003472",
        "debtor_name": 'ООО "ПКО "ЧелСИ""',
        "debtor_inn": "7453078689",
        "debtor_bank_name": "ОТДЕЛЕНИЕ N8597 СБЕРБАНКА РОССИИ Г ЧЕЛЯБИНСК",
        "currency_code": "643",
    }

    operations = [
        {
            "document_operation_date": "10.01.2013",
            "document_type_code": "01",
            "document_number": "25",
            "payer_or_recipient_name": 'ООО "КрАФТ-Пак"',
            "payer_or_recipient_inn": "6673214423",
            "payer_or_recipient_kpp": "667301001",
            "account_number": "40702810438050000548",
            "debit_amount": "250000-00",
            "credit_amount": "0",
            "payment_purpose": 'Оплата за мешки бумажные по договору № 30 от 30.08.2010г, сч.№303 от 28.12.12г. Сумма 250000-00 В т.ч. НДС(18%) 38135-59',
            "correspondent_account_number": "30101810100000000964",
            "payer_or_recipient_bank": 'ФИЛИАЛ "ЕКАТЕРИНБУРГСКИЙ" ОАО "АЛЬФА-БАНК" Г ЕКАТЕРИНБУРГ',
            "bank_bik": "046577964",
        }
    ]

    out = Path(__file__).parent / "uploads" / "smoke.xlsx"
    out.parent.mkdir(exist_ok=True)
    build_excel_file(header, operations, str(out))
    print(str(out))


if __name__ == "__main__":
    main()





































