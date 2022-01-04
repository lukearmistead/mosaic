#!/bin/bash

# Starts a quick note file
n() {
  NOTEDIRECTORY="$HOME/workspace/log/notes/"
  DATE=$(date  +"%Y-%m-%d-")
  TITLE=$1
  EXT=".txt"
  vim ${NOTEDIRECTORY}${DATE}${TITLE}${EXT}
}

# Opens todo file
alias td='vim $HOME/workspace/log/todo.txt'
