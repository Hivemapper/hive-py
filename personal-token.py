import base64

def get_personal_token():
    user_name = ""
    api_key = ""

    string_to_encode = f"{user_name}:{api_key}"
    encoded_bytes = base64.b64encode(string_to_encode.encode("utf-8"))
    encoded_string = encoded_bytes.decode("utf-8")

    return encoded_string

if __name__ == "__main__":
    print(get_personal_token())
