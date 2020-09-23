import re


out = b"Successfully submitted metadata object 9183465129133019626-242ac1111-0001-012".decode("utf-8")
match = re.fullmatch(r"Successfully submitted metadata object (.+)", out)

if match is not None:
    uuid = match.group(1)
    print(uuid)
else:
    print("failed")