from utils import mongo_init

db = mongo_init("epub_testing")
db2 = mongo_init("epub_wiley2")
oct_toc2=db2.oct_toc
oct_toc = db.oct_toc
oct_no_toc = db.oct_no_toc
oct_chapters = db.oct_chapters
oct_chapters2 = db2.oct_chapters


# handle toc
# exist=[]
# not_exist=[]
# for book in oct_toc2.find({}):
#     alreadyExist = oct_toc.find_one({"book":book['book']})
#     if alreadyExist:
#         exist.append(book['book'])
#     else:
#         oct_toc.insert_one(book)
#         not_exist.append(book['book'])

# print("total toc exist",len(exist))
# print("total toc not exist",len(not_exist))


# handle oct_chapters
# exist=[]
# not_exist=[]
# for book in oct_chapters2.find({}):
#     oct_chapters.insert_one(book)

# print("total oct_chapters exist",len(exist))
# print("total oct_chapter not exist",len(not_exist))
alreadyExist = oct_toc.find({})
book_list = list(alreadyExist)
# book_count = len(list(alreadyExist))
print(book_list[-2]['book'])