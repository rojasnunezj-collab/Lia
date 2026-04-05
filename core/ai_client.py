# ====================================================================
# --- IMPORTS ---
# ====================================================================
import asyncio
import vertexai
from vertexai.generative_models import GenerativeModel, SafetySetting, HarmCategory, GenerationConfig
from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable
from config.settings import PROJECT_ID, REGION_ESTABLE, MODEL_NAME

# ====================================================================
# --- INICIALIZACIÓN IA ---
# ====================================================================
model = None

def init_ai(credentials):
    global model
    vertexai.init(project=PROJECT_ID, location=REGION_ESTABLE, credentials=credentials)
    model = GenerativeModel(MODEL_NAME)

# ====================================================================
# --- GENERACIÓN IA Y REINTENTOS ---
# ====================================================================
async def generar_con_reintento(partes, prompt, msg, is_json=False):
    config = {"temperature": 0.1, "top_p": 0.1, "max_output_tokens": 8192}
    if is_json: config["response_mime_type"] = "application/json"
    else: config["response_mime_type"] = "text/plain" 
        
    safety_settings = [
        SafetySetting(category=HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE),
        SafetySetting(category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=SafetySetting.HarmBlockThreshold.BLOCK_NONE),
    ]
    for attempt in range(5):
        try:
            if model is None: raise ValueError("IA no inicializada.")
            await asyncio.sleep(1) 
            return await model.generate_content_async(partes + [prompt], generation_config=GenerationConfig(**config), safety_settings=safety_settings)
        except (ResourceExhausted, ServiceUnavailable):
            wait = 12 * (attempt + 1)
            await msg.edit_text(f"⏳ Saturación API. Reintento en {wait}s...")
            await asyncio.sleep(wait)
        except Exception as e: raise e
