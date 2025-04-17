#!/bin/bash

GO_EXECUTABLE="./client"
MOVIES_FILE="datasets/movies.csv"
RATINGS_FILE="datasets/ratings.csv"
CREDITS_FILE="datasets/credits.csv"

$GO_EXECUTABLE "$MOVIES_FILE" "$RATINGS_FILE" "$CREDITS_FILE"
