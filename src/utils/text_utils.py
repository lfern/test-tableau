

def fix_mojibake(text: str) -> str:
    try:
        # print("Texto:", text)
        # print("Hex:", text.encode("utf-8").hex())
        # try:
        #     print("Texto->:", text.encode("latin1").decode("utf-8"))
        # except:
        #     pass

        return text.encode("cp1252").decode("utf-8")
    except UnicodeEncodeError:
        return text  # ya está bien
    except UnicodeDecodeError:
        return text  # ya está bien
