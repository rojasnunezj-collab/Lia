# ====================================================================
# --- IMPORTS ---
# ====================================================================
import asyncio
from google import genai
from google.genai import types
from google.genai.errors import ClientError, APIError
from config.settings import PROJECT_ID, REGION_ESTABLE, MODEL_NAME, FALLBACK_MODELS, logger

# ====================================================================
# --- INICIALIZACIÓN IA ---
# ====================================================================
client = None
current_model = None

def init_ai(credentials=None):
    global client, current_model
    if not credentials:
        from core.sheets_client import obtener_credenciales
        credentials = obtener_credenciales()
    try:
        client = genai.Client(vertexai=True, project=PROJECT_ID, location=REGION_ESTABLE)
        current_model = MODEL_NAME
    except Exception as e:
        logger.error(f"Error inicializando IA: {e}")

# ====================================================================
# --- GENERACIÓN IA Y REINTENTOS ---
# ====================================================================
async def generar_con_reintento(partes, prompt, msg, is_json=False):
    global client, current_model
    
    response_mime_type = "application/json" if is_json else "text/plain"
    
    config = types.GenerateContentConfig(
        temperature=0.1,
        top_p=0.1,
        max_output_tokens=8192,
        response_mime_type=response_mime_type,
        safety_settings=[
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=types.HarmBlockThreshold.BLOCK_NONE),
            types.SafetySetting(category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=types.HarmBlockThreshold.BLOCK_NONE),
        ]
    )

    model_list = [MODEL_NAME] + FALLBACK_MODELS
    
    for attempt in range(5):
        try:
            if client is None: 
                init_ai()
            if client is None:
                raise ValueError("IA no inicializada y no se encontraron credenciales válidas.")
                
            if not current_model:
                current_model = MODEL_NAME
                
            await asyncio.sleep(1)
            
            contents = partes + [prompt]
            
            return await client.aio.models.generate_content(
                model=current_model,
                contents=contents,
                config=config
            )
        except ClientError as e:
            if "404" in str(e) or "not found" in str(e).lower() or "not have access" in str(e).lower():
                if current_model in model_list:
                    idx = model_list.index(current_model)
                    if idx + 1 < len(model_list):
                        current_model = model_list[idx + 1]
                        await msg.edit_text(f"⚠️ Modelo restringido. Cambiando a `{current_model}`...")
                        continue
                        
            if "429" in str(e) or "quota" in str(e).lower():
                wait = 12 * (attempt + 1)
                await msg.edit_text(f"⏳ Saturación API o Límite de Cuota. Reintento en {wait}s...")
                await asyncio.sleep(wait)
            else:
                raise e
        except APIError as e:
            if "429" in str(e) or "Quota" in str(e):
                wait = 12 * (attempt + 1)
                await msg.edit_text(f"⏳ Saturación API. Reintento en {wait}s...")
                await asyncio.sleep(wait)
            else:
                raise e
        except Exception as e:
            raise e
