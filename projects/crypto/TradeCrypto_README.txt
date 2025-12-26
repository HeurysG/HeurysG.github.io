TradeCrypto

DESCRIPTION
TradeCrypto is a user-friendly application designed to assist cryptocurrency investors in making informed decisions. This package provides a customizable and intuitive platform for visualizing portfolio performance, receiving personalized investment recommendations, and leveraging machine learning algorithms. With TradeCrypto, users can tailor their investment strategies based on individual preferences, risk tolerance, and market cap criteria.

INSTALLATION
1. Download data from Kraggle and extract it to 'data' folder https://www.kaggle.com/datasets/georgezakharov/historical-data-on-the-trading-of-cryptocurrencies
2. Install Python and pip
3. Install Node.js from https://nodejs.org/en
4. Go to 'api' folder and run pip install -r requirements.txt
5. Start the back-end by running (from api folder) python server.py
6. Go to 'client' folder and run: npm install
7. Start the React server by running (from client folder): npm start
8. Access the application on http://localhost:3000
DEMO INSTALLATION: https://youtu.be/4F5PVmEnWLY

EXECUTION
1. Select the date for which you would like the model to use data from
2. Set your risk tolerance as either Conservative, Moderate, or Aggressive
3. Set the minimum and maximum market capitalization, we suggest setting a range from 1 billion to 1 trillion USD
4. Click the "Train Models" button at the bottom of the page to add in your current assets
5. Add in your current crypto holdings
6a. For seeing your current portfolio performance click the "Analyze Performance" button at the bottom of the page and then 
click on "Portfolio Performace" tab on the sidebar
6b. To run the models to find the investment suggestions click the "Generate Insights" button at the bottom of the page and 
then click on "Portfolio Insights" tab on the sidebar 
7. From the Portfolio Insights tab you can add the suggestions to your portfolio, change percentage holding, and re-run the 
analysis by clicking the "Start Analysis" to see how the suggestions would affect your portfolio. 