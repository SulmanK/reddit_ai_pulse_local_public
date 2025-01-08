"""
Gemini Analysis Module

This module uses Google's Gemini AI to analyze Reddit data, generating insights
and summaries from processed posts and comments.

Owner: Sulman Khan
"""

import os
import sys
import logging
import psycopg2
import google.generativeai as genai
from datetime import datetime
from typing import Dict

# Add the Local directory to Python path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.append(LOCAL_DIR)

from config.config import get_database_config, SUBREDDITS, GOOGLE_GEMINI_API_KEY
from utils.custom_logging import setup_logging

# Initialize Google Gemini API
genai.configure(api_key=GOOGLE_GEMINI_API_KEY)

def create_prompt_template():
    """Return the standard prompt template for Gemini analysis."""
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    return f"""
    Analyze the provided text files, which contain Reddit posts and comments.
    **Instructions:**
    1.  **Content Filtering:** Before any analysis, check the provided text files for the presence of harassment, hate speech, or explicit material. If such material is detected, **do not proceed with any further summarization or analysis for that particular text file. Instead, output a single line stating `Analysis Skipped: Content contains harmful material.`**
    2.   "Do not include any hate speech, harassment, or offensive language in any analysis if content does not contain harmful material."
    3.  **Ranking:** Rank the threads based on their total "Score" from highest to lowest.
    4.  **Summarization:** Utilize the provided "Summary" fields to give a brief overview of what people were discussing in each thread.
    5.  **Emotional Analysis:** Use the "Emotion Label" and "Emotion Score" to discuss the overall emotional tone of each thread. Identify any dominant emotions, and note variations.
    6.  **Point of View Extraction:** For each thread, try to extract and summarize the top 3 points of view or arguments made by different users. If there are fewer than 3 distinct viewpoints, just list what you can find.
    7.  **Links:** Embed the URL field inside of the thread titles, using Markdown.
    8.  **Output Format:** Analyze the provided text files and output in this exact format:
        ---
        title: "{{subreddit_name}} subreddit"
        date: "{current_date}"
        description: "Analysis of top discussions and trends in the {{subreddit_name}} subreddit"
        tags: ["tag1", "tag2", "tag3"]
        ---

        # Overall Ranking and Top Discussions
        *   A numbered list of the threads, ranked by their "Score", with the highest score first.
        *   Each entry should include the thread's name (with embedded link), score and a short explanation of what was discussed. Make sure the short explanation is a bullet point (it should be under each thread)
        *   Format the title with its tag and link as follows:
            * If title contains [X] tag (like [D], [R], [P], [Project], [Discussion], [Research], etc.):
                `[[X] Rest of Title](post_url)`
            * If no tag:
                `[Title](post_url)`
            Examples:
                * Title with tag: `[[D] Machine Learning Updates](https://reddit.com/...)`
                * Title with multiple tags: `[[D] [R] Machine Learning Updates](https://reddit.com/...)`
                * Title with tag at the end: `[Machine Learning Updates [D]](https://reddit.com/...)`
                * Title without tag: `[Database Performance Guide](https://reddit.com/...)`
        
        # Detailed Analysis by Thread 
        *   Each analysis should follow this format:
            **[ {{post_title}} (Score: {{Score}})](URL)**
            *  **Summary:**  {{The summary of the thread}}
            *  **Emotion:** {{The emotional tone of the thread}}
            *  **Top 3 Points of View:**
                * {{Point of view 1}}
                * {{Point of view 2}}
                * {{Point of view 3}}

    **Provided Text Files:**
    [Insert the content of your text files here, each block prefixed with "~~~" to indicate a text file]
    **Note:** If no text files are supplied, simply state: "It was very quiet in that subreddit today. Please adhere to the output format."
    """

def format_text_file(row):
    """Format a single row of data into text file format."""
    return f"""~~~
    post_id: {row[0]}
    subreddit: {row[1]}
    post_score: {row[2]}
    post_url: {row[3]}
    comment_id: {row[4]}
    summary_date: {row[5]}
    post_content: {row[6]}
    comment_body: {row[7]}
    comment_summary: {row[8]}
    sentiment_score: {row[9]}
    sentiment_label: {row[10]}
    """

def get_formatted_subreddit_name(subreddit: str) -> str:
    """
    Returns a properly formatted subreddit name for display.
    
    Args:
        subreddit (str): The raw subreddit name
        
    Returns:
        str: The formatted subreddit name
    """
    subreddit_formats = {
        "claudeai": "ClaudeAI",
        "dataengineering": "Data Engineering",
        "datascience": "Data Science",
        "localllama": "LocalLLaMA",
        "machinelearning": "Machine Learning",
        "openai": "OpenAI",
        "singularity": "Singularity",
        "stablediffusion": "Stable Diffusion"
    }
    return f"{subreddit_formats.get(subreddit.lower(), subreddit)} Subreddit"

def process_subreddit(model, cur, subreddit, output_dir):
    """Process a single subreddit's data."""
    logging.info(f"Analyzing data for subreddit: {subreddit}")
    
    # Fetch data
    cur.execute("""
        SELECT post_id, subreddit, post_score, post_url, comment_id,
               summary_date, post_content, comment_body, comment_summary,
               sentiment_score, sentiment_label
        FROM processed_data.joined_summary_analysis
        WHERE subreddit = %s
        ORDER BY post_score DESC
    """, (subreddit,))
    rows = cur.fetchall()

    if not rows:
        logging.info(f"No data found for subreddit: {subreddit}")
        return

    # Prepare prompt
    text_files = "".join(format_text_file(row) for row in rows)
    final_prompt = create_prompt_template() + text_files

    # Format the response after getting it from Gemini
    try:
        response = model.generate_content(final_prompt)
        formatted_response = response.text
        
        # Replace both title and description with proper formatting
        formatted_subreddit = get_formatted_subreddit_name(subreddit)
        formatted_response = formatted_response.replace(
            f'title: "{subreddit} subreddit"',
            f'title: "{formatted_subreddit}"'
        ).replace(
            f'description: "Analysis of top discussions and trends in {subreddit} subreddit"',
            f'description: "Analysis of top discussions and trends in {formatted_subreddit}"'
        )
        
        output_file_path = os.path.join(output_dir, f"llm_{subreddit}.md")
        
        with open(output_file_path, "w", encoding="utf-8") as f:
            f.write(formatted_response)
        logging.info(f"Output saved to {output_file_path}")
        
    except Exception as e:
        logging.error(f"Error processing subreddit {subreddit}: {e}")

def clean_markdown_file(file_path):
    """
    Remove triple backticks and clean up resulting whitespace from markdown file
    while preserving markdown formatting.
    
    Args:
        file_path (str): Path to the markdown file
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        # Clean lines and remove empty lines that resulted from backtick removal
        cleaned_lines = []
        for line in lines:
            # Remove backticks and trim whitespace
            cleaned_line = line.replace('```', '').rstrip()
            # Only add non-empty lines or lines that are intentionally blank (part of markdown)
            if cleaned_line or line.strip() == '':
                cleaned_lines.append(cleaned_line + '\n')
        
        # Write cleaned content back to file
        with open(file_path, 'w', encoding='utf-8') as f:
            f.writelines(cleaned_lines)
        
        logging.info(f"Successfully cleaned markdown file: {file_path}")
    except Exception as e:
        logging.error(f"Error cleaning markdown file {file_path}: {e}")
        raise

def analyze_data():
    """
    Main function to analyze Reddit data using Google's Gemini model.
    
    Pipeline steps:
    1. Set up logging and output directory with current date
    2. Initialize Gemini model
    3. Connect to database
    4. Process each subreddit
    5. Clean generated markdown files
    """
    setup_logging()
    conn = None
    cur = None
    
    try:
        # 1. Set up output directory with year/month/day structure
        current_date = datetime.now()
        year = current_date.strftime('%Y')
        month = current_date.strftime('%m')
        day = current_date.strftime('%d')
        
        output_dir = os.path.join('/opt/airflow/results', year, month, day)
        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Output directory set to {output_dir}")

        # 2. Initialize model
        model = genai.GenerativeModel('gemini-2.0-flash-exp')
        logging.info("Model loaded")

        # 3. Connect to database
        db_config = get_database_config()
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        logging.info("Database connection established")

        # 4. Process subreddits
        for subreddit in SUBREDDITS:
            process_subreddit(model, cur, subreddit, output_dir)
            
        # 5. Clean markdown files
        for filename in os.listdir(output_dir):
            if filename.endswith('.md'):
                file_path = os.path.join(output_dir, filename)
                clean_markdown_file(file_path)

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    analyze_data()