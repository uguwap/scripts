from recovery_parser import recover_operations_from_batch


def main() -> None:
    batch = "\n".join(
        [
            'Строка 102: │  30│ 10.01.13 │ 01 │       25│ 09.01.13 │30101810100000000964│ФИЛИАЛ"ЕКАТЕРИНБУРГСКИЙ"      │046577964│ИНН 6673214423 КПП 667301001 ООО ПФ "КрАФТ-Пак    │6673214423  │667301001│40702810438050000548│     250000-00│              │Оплата за мешки бумажные по договору № 30│',
            'Строка 103: │    │          │    │         │          │                    │ОАО"АЛЬФА-БАНК" Г ЕКАТЕРИНБУРГ│         │2000"                                             │            │         │                    │              │              │30.08.2010г, сч.№303 от 28.12.12г.│',
        ]
    )
    ops = recover_operations_from_batch(batch)
    print(len(ops))
    if ops:
        print(
            ops[0].get("source_line"),
            ops[0].get("document_operation_date"),
            ops[0].get("debit_amount"),
            ops[0].get("credit_amount"),
            ops[0].get("payer_or_recipient_inn"),
        )


if __name__ == "__main__":
    main()







