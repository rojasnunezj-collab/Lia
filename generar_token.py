from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/cloud-platform"
]

def main():
    print("Iniciando generación de token.json usando tu propia cuenta de Google...")
    print("Se abrirá una ventana en tu navegador web. Por favor, inicia sesión y autoriza a la aplicación.")
    
    flow = InstalledAppFlow.from_client_secrets_file('client_secret.json', SCOPES)
    creds = flow.run_local_server(port=0)
    
    with open('token.json', 'w') as token:
        token.write(creds.to_json())
        
    print("\n✅ ¡ÉXITO! El archivo 'token.json' se ha generado en esta carpeta.")
    print("Puedes presionar cualquier tecla para salir.")

if __name__ == '__main__':
    main()
