import os
import streamlit as st
import psycopg2
from sentence_transformers import SentenceTransformer
import pandas as pd

# PostgreSQL connection settings from environment variables
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

@st.cache_resource
def load_model():
    return SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

@st.cache_resource
def get_db_connection():
    # Cache the connection itself to avoid connecting on every search
    return psycopg2.connect(**DB_CONFIG)

def find_similar_reviews(query_text, top_n=5):
    # Model should be cached to avoid reloading
    model = load_model()
    query_embedding = model.encode(query_text).tolist()

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT customer_name, review, (review_embedding <=> %s::vector) AS similarity
                FROM public.reviews
                ORDER BY similarity ASC
                LIMIT %s;
                """,
                (query_embedding, top_n),
            )
            results = cursor.fetchall()
        return results
    except Exception as e:
        st.error(f"Database error: {e}")
        return []
    finally:
        pass # connection remains open and cached by Streamlit

st.set_page_config(page_title="Customer Review Search", layout="wide")
st.title("🔍 Search Customer Reviews by Similarity")

query = st.text_area("Enter your review or search text:", "")
top_n = st.slider("Number of similar reviews to show", min_value=1, max_value=10, value=5)

if st.button("Search") and query.strip():
    with st.spinner("Searching for similar reviews..."):
        results = find_similar_reviews(query, top_n)
        if results:
            df = pd.DataFrame(results, columns=["Customer Name", "Review", "Similarity"])
            df["Similarity"] = df["Similarity"].apply(lambda x: f"{x:.4f}")
            st.dataframe(df)
        else:
            st.info("No similar reviews found.")
