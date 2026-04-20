import config
import fitz
import os

def decompose_tag(tag, tag_system_prefix):
    if tag[:len(tag_system_prefix) + 1] == tag_system_prefix + "-":
        start_pos=len(tag_system_prefix) + 1
    else:
        start_pos=len(tag_system_prefix) 
    pos_1 = tag.find("1")
    equip_cat=tag[start_pos:pos_1].replace("-","")
    unit = tag[pos_1:pos_1+2]
    if len(tag[pos_1:].replace("-",""))>5:
        package_letter = tag[pos_1+2]
        tag_number = tag[pos_1+3:pos_1+6]
        suffix = tag[pos_1+6:].replace("-","")
    else:
        package_letter = ""
        tag_number = tag[pos_1+2:pos_1+4]
        suffix = tag[pos_1+4:].replace("-","")
    return (equip_cat, unit, tag_number, suffix)
    

def file_treatment(connection, cursor, eqdb_decomposed):
    doc_reg = {}
    document_revisions = {}
    all_entries = os.listdir(config.directory)
        # Filter the list to include only actual files
    only_files = [
            entry for entry in all_entries 
            if os.path.isfile(os.path.join(config.directory, entry))
        ]
    cursor.execute(sql.SQL_CREATE_DOC_TABLE)
    cursor.execute(sql.SQL_CREATE_CLDT_TABLE)
    cursor.execute(sql.SQL_CREATE_ERRORS_TABLE)
    connection.commit()
    workbook = openpyxl.load_workbook(config.excel_file_path,data_only=True)
    sheet = workbook[config.cldt_sheet_name]
    imported_cldt = list(sheet.iter_rows(7,sheet.max_row,2,6, values_only=True))
    imported_cldt = [row for row in imported_cldt if decompose_tag(row[4]) in eqdb_decomposed]
    for file_name in only_files:
        file_path = os.path.join(config.directory,file_name)
        pdf_file = fitz.open(file_path)
        content_of_title_page = pdf_file[0].get_text("words",sort=False)
        doc_number_found = False
        revision_found = False
        for word in content_of_title_page:
            if (config.FILE_NUMBER_START in word[4]) and (len(word[4])==22):
                document_number = word[4]
                doc_number_found = True
            elif word[4]=="REV":
                rev_x_pos = word[0]
                rev_y_pos = word[1]
                received_y_pos = rev_y_pos - 200
                revision_found = True
        for word in content_of_title_page:
            if (word[0]> rev_x_pos-10) and (word[0] < rev_x_pos +10):
                if (word[1]<rev_y_pos) and (word[1]>received_y_pos):
                    rev_y_pos = word[1]
                    document_revisions[document_number] = word[4]
        if not(doc_number_found and revision_found):
            document_number = file_name[:22]
            document_revisions[document_number] = file_name[-8:-6]
        treat_document = insert_or_update_document_revision(connection, document_number, document_revisions[document_number])
        if not(treat_document):
            continue
        #% tags extraction
        cursor.execute(sql.SQL_DELETE_PREVIOUS_TAGS, (document_number,))
        connection.commit()
        tags_found = set()
        list_suspect = []
        page_num = 1
        for page in pdf_file:
            content_of_page = page.get_text("words",sort=False)
            if page_num == 1:
                matrix = page.rotation_matrix
            for word in content_of_page:
                if not(word[4] in tags_found):
                    word_decomposed = logic.decompose_tag(word[4])
                    if (word[4] in eqdb_tags):
                        tags_found.add(word[4])
                    elif word_decomposed in eqdb_decomposed:
                        tags_found.add(eqdb_dict[word_decomposed])
                        list_suspect.append([document_number, document_revisions[document_number], page_num,  word[4] , eqdb_dict[word_decomposed]])
                    elif (len(word[4]) in {4, 5, 6}) and (word[4][:2]==config.TAG_SYSTEM_PREFIX):
                        if page.rotation_matrix == matrix:
                            ending_coord = [word[0]-5,word[3],word[2]+5,2*word[3]-word[1]]                    
                        else:
                            ending_coord = [word[2],word[1]-5,2*word[2]-word[0],word[3]+5]                                
                        ending = page.get_textbox(ending_coord)
                        if ending[-2:]=="\n+":
                            ending = ending[:-2]
                        instrum_word = word[4]+ending
                        instrum_tag_decomposed=logic.decompose_tag(instrum_word)
                        if instrum_word in eqdb_tags:
                            tags_found.add(instrum_word)
                        elif instrum_tag_decomposed in eqdb_decomposed:
                            tags_found.add(eqdb_dict[instrum_tag_decomposed])
                            list_suspect.append([document_number, document_revisions[document_number], page_num,  instrum_word , eqdb_dict[instrum_tag_decomposed]])                                   
            page_num += 1
        tags_namrata = [row[4] for row in imported_cldt if row[0] == document_number]
        tags_found.update(tags_namrata)
        cldt_list = [[document_number, "000", document_revisions[document_number],"Tag",item] for item in tags_found]
        cursor.executemany(sql.SQL_INSERT_CLDT, cldt_list)
        connection.commit()
        cursor.executemany(sql.SQL_INSERT_ERRORS, list_suspect)
        connection.commit()
        doc_reg[document_number] = tags_found
    treated_files = get_set_from_db(connection,
                                    "document_versions",
                                    "doc_id")
    cldt_list = []
    for i in range(len(imported_cldt)):
        doc_num = imported_cldt[i][0] 
        if (doc_num not in treated_files):
            if (doc_num != imported_cldt[i-1][0]):
                update = insert_or_update_document_revision(connection, doc_num, imported_cldt[i][2])
            cldt_list.append(imported_cldt[i])
    cursor.executemany(sql.SQL_INSERT_CLDT, cldt_list)
    connection.commit()
