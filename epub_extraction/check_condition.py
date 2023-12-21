from bs4 import BeautifulSoup

html = """
<p>
  <img>
</p>
<h class="example-class">fdd</h>
<p>caption</p>
"""

soup = BeautifulSoup(html, 'html.parser')

# Assuming you have a reference to the first <p> tag
current_child = soup.find('p')

# Find the next sibling tag
next_sibling = current_child.find_next_sibling()

if next_sibling:
    if 'class' in next_sibling.attrs:
        print("Next sibling class:", next_sibling['class'])
    else:
        print("Next sibling has no class.")
else:
    print("No next sibling tag found.")
