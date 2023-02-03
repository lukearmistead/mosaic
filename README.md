A tool supporting data-driven reflection. Arete pulls data from a variety of sources and summarizes it for self reviews.


To get access tokens from Plaid, follow this tutorial: https://youtu.be/sGBvKDGgPjc

Road map:

Ideas
- Relative date handler for config
- Make endpoint-specific configurations for extracts

TASK MANAGEMENT REQS:
- Edit from terminal
- Bullet journal behavior
- Append from command line
- Append and view from mobile
- Create, update, delete tracked - is this really a requirement?

JOURNAL REQS:
- Edit from terminal
- Track state changes
- What attributes actually need to be exposed for external use?
- If the `access_token` variable hangs around for days, it may not be fresh.
    - Write a function `get_access_token` which checks for freshness before providing access to the variable.
- Add logging to explain completed tasks like writing files
- Is there an elegant way to deal with the file creation, deletion, and shuffle?
- Creation and completion timestamps for todos
- Command line interface to add, delete, list, and modify tasks
- Add and view tasks from phone
- Add readability to task symbols. Are there emojis that are more distinctive?
- Current week notes and tasks go in home directory
- Log of past weekly notes and tasks go in log directory with each week's review
    - Should this be organized by week or month?
- Long-standing reference materials go in reference directory
