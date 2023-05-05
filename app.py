"""Streamlit app using Snowpark and Data from the Snowflake Marketplace (Knoema Economy Data Atlas)"""

# Import Snowpark functions
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, lag
from snowflake.snowpark.window import Window

# Import libraries
import streamlit as st
import pandas as pd
import openai


# Define functions
def create_session_object():
    """Create Snowpark session object"""
    connection_parameters = st.secrets["snowflake"]
    session = Session.builder.configs(connection_parameters).create()
    return session


# Function to show economic data
def show_econ_data(session_econ):
    """Display economic variables for the US in a line chart"""
    # Load and transform inflation data via Snowpark API
    snow_df_inflation = session_econ.table("ECONOMY.BEANIPA").filter((col('Table Name') == 'Price Indexes For Personal Consumption Expenditures By Major Type Of Product')
                                                                             & (col('Indicator Name') == 'Personal consumption expenditures (PCE)')
                                                                             & (col("\"Frequency\"") == 'Q'))
    snow_df_inflation = snow_df_inflation.select(col("\"Date\"")
                                                        ,col("\"Value\"").alias("PriceIndex")
                                                        ,lag("\"Value\"", offset=3).over(Window.partition_by().order_by(col("\"Date\""))).alias("YearBefore")
                                                        ,(col("PriceIndex") - col("YearBefore")).alias("Diff")
                                                        ,(col("Diff") * 100 / col("YearBefore")).alias("Inflation")).order_by("\"Date\"")
    snow_df_inflation = snow_df_inflation.select(col("\"Date\""), col("Inflation"))

    # Load unemployment data via Snowpark API
    snow_df_unemployment = session_econ.table("ECONOMY.BLSUSLFSCPS2019").filter((col('Series Name') == 'Unemployment Rate - (Seas)') & (col("\"Frequency\"") == 'Q'))
    snow_df_unemployment = snow_df_unemployment.select(col("\"Date\"")
                                                      ,col(("\"Value\"")).alias("Unemployment")).order_by("\"Date\"")

    # Load and transform GDP growth data via Snowpark API
    snow_df_gdp = session_econ.table("ECONOMY.BEANIPA").filter((col("Table Name") == 'Gross Domestic Product')
                                                                & (col("Indicator Name") == 'Gross domestic product, A191RC-1')
                                                                & (col("\"Frequency\"") == 'Q')
                                                                ).order_by("\"Date\"")
    snow_df_gdp = snow_df_gdp.select(col("\"Date\"")
                                    ,col("\"Value\"").alias("GDP")
                                    ,lag("\"Value\"", offset=3).over(Window.partition_by().order_by(col("\"Date\""))).alias("YearBefore")
                                    ,(col("GDP") - col("YearBefore")).alias("Diff")
                                    ,(col("Diff") * 100 / col("YearBefore")).alias("Growth")).order_by("\"Date\"")
    snow_df_gdp = snow_df_gdp.select(col("\"Date\""), col("Growth"))

    # Convert dataframes to pandas
    pd_inflation = snow_df_inflation.to_pandas()
    pd_unemployment = snow_df_unemployment.to_pandas()
    pd_growth = snow_df_gdp.to_pandas()
    
    # Merge datasets together
    pd_econ = pd.merge(left=pd_inflation, right=pd_unemployment, how="inner", on="Date")
    pd_econ = pd.merge(left=pd_econ, right=pd_growth, how="inner", on="Date")

    # Let the user choose which variables to show and display them over time in a line chart
    options = st.multiselect("Which indicators do you want to show?", ("INFLATION", "UNEMPLOYMENT", "GROWTH"))
    with st.container():
        st.line_chart(data=pd_econ, x="Date", y=options)


# Define functions to generate text from ChatGPT (code is partly borrowed from https://github.com/kinosal/tweet)
openai.api_key = st.secrets["OPEN_API_KEY"]
def complete(prompt: str, temperature: float = 0.9, max_tokens: int = 500) -> str:
    """Let ChatGPT build an answer"""
    kwargs = {
        "engine": "text-davinci-003",
        "prompt": prompt,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "top_p": 1,  # default
        "frequency_penalty": 0,  # default,
        "presence_penalty": 0,  # default
    }
    try:
        response = openai.Completion.create(**kwargs)
        return response["choices"][0]["text"]

    except Exception as e:
        st.session_state.text_error = f"OpenAI API error: {e}"


def ask_gpt(question: str, economist: str, placeholder, answer) -> str:
    """Use ChatGPT to ask about what an economist would have thought about the user's question"""
    # Generate Chat-GPT answer
    if st.session_state.n_requests >= 5:
        st.session_state.text_error = "Please wait a few seconds before asking another question."
        st.session_state.n_requests = 1
        return

    st.session_state.answer = ""
    st.session_state.text_error = ""

    if not question:
        st.session_state.text_error = "Please enter a question"
        return

    with placeholder:
        with st.spinner(f"Please wait while the GPT spirit of {economist} is thinking about your question..."):
            prompt = f"What would {economist} have said about the following question: {question}"
            st.session_state.text_error = ""
            st.session_state.n_requests += 1
            st.session_state[answer] = complete(prompt).strip()


# Build the Streamlit page
if __name__ == "__main__":

    # Set the page configuration
    st.set_page_config(
        page_title="EconGPT",
        page_icon="",
        layout="wide",
        initial_sidebar_state="expanded",
        menu_items={
            'About': "This is a Streamlit app for the Streamlit Hackathon using Snowpark for Python, Streamlit, Openai and Snowflake Data Marketplace (Knoema: ECONOMY DATA ATLAS)"
        }
    )
    
    # Initialize state variables
    if "answer" not in st.session_state:
        st.session_state.answer = ""
    if "text_error" not in st.session_state:
        st.session_state.text_error = ""
    if "n_requests" not in st.session_state:
        st.session_state.n_requests = 0


    # Add header and a subheader
    st.header("EconGPT: Chat with your favorite economist!")
    st.subheader("Powered by Snowpark for Python, ChatGPT and the Knoema Economy Data Atlas from Snowflake Marketplace | Made with Streamlit")
    st.markdown("""Have you ever wondered about how different schools of thought in the field of economics view the development of key economic indicators?
                    Do you ask yourself whether today's economic policy rather follows the famous macroeconomist John Keynes' ideas or still is in line with who is sometimes referred to as the father of economics Adam Smith?
                    \n What would they have said about the role of the government during a global pandemic, would their opinions differ on the reasons for today's inflation? 
                    \n In this mini-app you can have a detailed look at the development of some key macroeconomic indicators over the last decades in the US and can ask your favorite economists (or rather "ask" their ChatGPT spirits)
                about potential reasons, economic explanations or policy advice (or anything else you want).  
                \n _Have fun! :)_""")
    st.divider()
    
    # Show economic data
    session_econ = create_session_object()
    show_econ_data(session_econ)

    # Divider between data and ChatGPT part
    st.divider()

    # New part of the page for ChatGPT
    st.subheader("Ask your favorite Economist!")
    st.markdown("""How about asking some of the most famous economists, or rather their ChatGPT sprits, about these economic variables and thereby get an view into the different schools of thoughts these economists represent?   
                \n For example, ask Milton Friedman and John Maynard Keynes: _What is the role of the government in times of high unemployment?_""")
    
    # Text field for the question
    question = st.text_input(label="Your question:", placeholder="Text")

    # Display two columns, let the user choose two economists and show "their" answers to compare
    economists = ["Adam Smith (1723-1790)", "David Ricardo (1772-1823)", "John Maynard Keynes (1883-1946)", "Milton Friedman (1912-2006)"]
    col1, col2 = st.columns(2)
    with col1:
        economist1 = st.selectbox("Choose an economist", options=economists, key="select1")
        placeholder_answer1 = st.empty()
        answer1 = "answer1"
        st.session_state.answer1 = ""
        st.button(
            label="Ask the economist!",
            type="primary",
            on_click=ask_gpt(question, economist1, placeholder_answer1, answer1),
            key="button1"
        )
        if st.session_state.answer1:
            st.text_area(label=f"What {economist1} thinks about your question:", value=st.session_state[answer1], height=200)
    with col2:
        economist2 = st.selectbox("Choose an economist", options=economists, key="select2")
        placeholder_answer2 = st.empty()
        answer2 = "answer2"
        st.session_state.answer2 = ""
        st.button(
            label="Ask the economist!",
            type="primary",
            on_click=ask_gpt(question, economist2, placeholder_answer2, answer2),
            key="button2"
        )
        if st.session_state.answer2:
            st.text_area(label=f"What {economist2} thinks about your question:", value=st.session_state.answer2, height=200)

    # Horizontal bar at the bottom of the page
    st.markdown("""---""")
    st.markdown("This app uses US economic data from [Knoema](https://knoema.com/atlas/topics/Economy) on [Snowflake Marketplace](https://www.snowflake.com/en/data-cloud/marketplace/) and OpenAI's GPT-3 based [Davinci model](https://beta.openai.com/docs/models/overview). The code can be found on [GitHub](https://github.com/lstruth/StreamlitSnowflakeHackathon/).")
