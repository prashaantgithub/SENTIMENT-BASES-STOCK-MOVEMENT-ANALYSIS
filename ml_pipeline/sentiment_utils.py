import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# Global variables to cache model in memory so we don't reload it every time
_tokenizer = None
_model = None

def load_finbert_model():
    """
    Loads the FinBERT model from HuggingFace. 
    Uses global variables to ensure we only load it once per process.
    """
    global _tokenizer, _model
    
    if _model is None:
        print("⏳ Loading FinBERT model (this may take a moment)...")
        try:
            model_name = "ProsusAI/finbert"
            _tokenizer = AutoTokenizer.from_pretrained(model_name)
            _model = AutoModelForSequenceClassification.from_pretrained(model_name)
            _model.eval()  # Set to evaluation mode to disable dropout
            print("✅ FinBERT model loaded successfully.")
        except Exception as e:
            print(f"❌ Error loading FinBERT: {e}")
            raise e

    return _tokenizer, _model

def get_finbert_sentiment(text):
    """
    Analyzes text using FinBERT and returns the sentiment label and confidence score.
    
    Args:
        text (str): The financial text/headline.
        
    Returns:
        tuple: (label, score) -> e.g., ('positive', 0.98) or ('neutral', 0.0)
    """
    if not text or len(str(text).strip()) == 0:
        return "neutral", 0.0

    tokenizer, model = load_finbert_model()

    try:
        # Tokenize input
        inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
        
        # Run inference
        with torch.no_grad():
            outputs = model(**inputs)
        
        # Apply softmax to get probabilities
        probabilities = F.softmax(outputs.logits, dim=1)
        
        # Get the highest probability class
        confidence, predicted_class = torch.max(probabilities, dim=1)
        
        # FinBERT labels order: positive, negative, neutral
        labels = ["positive", "negative", "neutral"]
        sentiment = labels[predicted_class.item()]
        score = confidence.item()

        return sentiment, score

    except Exception as e:
        print(f"⚠️ Error in sentiment analysis: {e}")
        return "neutral", 0.0

if __name__ == "__main__":
    # Simple test to verify model loads correctly
    print("Testing FinBERT Module...")
    test_text = "The company reported a record breaking profit this quarter."
    label, score = get_finbert_sentiment(test_text)
    print(f"Text: {test_text}")
    print(f"Sentiment: {label}, Score: {score:.4f}")