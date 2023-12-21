from utils import mongo_init, latext_to_text_to_speech
import requests

db= mongo_init('epub_testing')
oct_chapters=db.oct_chapters

# Find all documents in oct_chapters
all_documents = oct_chapters.find({})

for document in all_documents:
    # Keep track of modified sections
    modified_sections = []

    # Iterate over sections
    for section in document.get("sections", []):
        equations = section.get("equations", [])

        # Check if section has equations
        if equations:
            # Iterate over equations
            for equation in equations:
                math_tag = equation.get("math_tag")
                api_url = 'http://localhost:9007'
                # Check if equations array is not empty and math_tag exists
                if equations and math_tag:
                    # Call your API to get LaTeX code
                    response = requests.post(api_url, json={"math_tag": math_tag})

                    if response.status_code == 200:
                        latex_code = response.json().get("data")

                        # Update equation object
                        equation["text"] = latex_code
                        text_to_speech = latext_to_text_to_speech(latex_code)
                        equation["text_to_speech"] = text_to_speech
                        equation.pop("math_tag", None)

            # Update only the "equations" array within the section
            section["equations"] = equations

        # Add modified section to the list
        modified_sections.append(section)

    # Update MongoDB with modified sections
    oct_chapters.update_one(
        {"_id": document["_id"]},
        {"$set": {"sections": modified_sections}}
    )