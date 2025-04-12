📺 YouTube Data Harvesting and Warehousing using SQL and Streamlit
📌 Project Overview
This project enables users to extract, manage, and analyze YouTube channel data using the YouTube Data API v3. The data is stored in a SQL data warehouse (MySQL/PostgreSQL), and the entire functionality is provided via an interactive Streamlit web application.

🧠 Skills Gained
Python Scripting

API Integration (YouTube Data API)

Data Management (SQL: MySQL / PostgreSQL)

Streamlit Application Development

SQL Queries and Data Analysis

🧰 Tech Stack
Python (Pandas, Requests, SQLAlchemy)

Streamlit (Frontend Web App)

YouTube Data API v3

MySQL / PostgreSQL (Data Storage)

Pickle (For temporary storage)

🎯 Problem Statement
Build a user-friendly Streamlit app that:

Accepts a YouTube Channel ID and fetches channel + video data using the YouTube API

Allows users to store data for up to 10 channels

Saves and manages the data using MySQL or PostgreSQL

Offers a way to query and analyze the data using pre-defined SQL queries

Displays query results and insights directly within the app

🧭 Project Workflow
📋 Step-by-Step Workflow
User inputs a YouTube Channel ID in the Streamlit app.

API Request is sent to the YouTube Data API v3 to collect:

Channel details (name, description, subscriber count, etc.)

Playlist ID (upload playlist)

All video IDs from the playlist

Each video’s metadata (views, likes, duration, etc.)

Top-level comments on each video

Data is temporarily stored in memory using Pandas DataFrames.

Data Cleaning & Structuring:

Missing/null values handled

Columns renamed/formatted

Nested structures (comments, tags) flattened

Data is migrated to SQL database (MySQL or PostgreSQL):

Channel table

Playlist table

Video table

Comment table

SQL Queries are executed based on user selections in the app:

Example: Most viewed videos, channel with most uploads, etc.

Query results are displayed in Streamlit as interactive tables.

(Optional): Export results or visualize them using graphs.
