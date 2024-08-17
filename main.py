import asyncio
import datetime
import pathlib

import bs4
import requests
import pandas as pd
import tqdm.asyncio
from bs4 import BeautifulSoup


async def process_comp_link(comp_link: str, comp_name: str, session_folder, sem: asyncio.Semaphore) -> list | None:
    session_folder = pathlib.Path(session_folder)
    loop = asyncio.get_event_loop()
    async with sem:
        tqdm.asyncio.tqdm_asyncio.write(f"\nОбрабатываем список \"{comp_name}\" ({comp_link})", file=None)
        comp_resp = await loop.run_in_executor(None, requests.get, comp_link)
    comp_page = BeautifulSoup(comp_resp.content.decode('utf-8'), "html.parser")
    if 'уровень подготовки - магистратура' not in comp_page.text.lower():
        print(f"Не магистратура, пропускаем")
        return None
    if 'форма обучения - очная' not in comp_page.text.lower():
        print(f"Не очная, пропускаем")
        return None
    if 'основание поступления - бюджетная основа' not in comp_page.text.lower():
        print(f"Не бюджет, пропускаем")
        return None
    start_pos_elem = comp_page.find('tr', attrs={'class': 'R13'})
    students_elements = start_pos_elem.find_all_next('tr', attrs={'class': 'R0'})
    students_rows = []
    for student_element in students_elements:
        stud_params_elements = student_element.find_all('td')
        stud_params = [elem.text.strip() for elem in stud_params_elements]
        stud_id = str(stud_params[1]).strip()
        stud_id = stud_id.replace('-', '').replace(' ', '')
        stud_id = f"{stud_id[0:3]}-{stud_id[3:6]}-{stud_id[6:9]}-{stud_id[9:]}"
        stud_sum_score = int(stud_params[2])
        stud_extra_score = int(stud_params[5])
        stud_docs_ready = str(stud_params[6])
        if 'ориг' in stud_docs_ready.lower():
            stud_docs_ready = 'Оригинал'
        else:
            stud_docs_ready = 'Копия'
        stud_priority = int(stud_params[7])
        stud_row = [stud_id, stud_sum_score, stud_extra_score, stud_priority, stud_docs_ready]
        students_rows.append(stud_row)
        pass
    pass
    columns_names = ['СНИЛС', 'Сумма баллов', 'Дополнительные баллы', 'Приоритет', 'Вид документов']
    students_df = pd.DataFrame(students_rows, columns=columns_names)

    with open(session_folder / f"{comp_name}.csv", 'w', encoding='utf-8') as f:
        students_df.to_csv(f, index=False, sep=';', encoding='utf-8', lineterminator='\n')


async def timacad_crawler():
    url = "https://www.timacad.ru/incoming/spiski-lits-podavshikh-dokumenty"
    resp = requests.get(url)
    page = BeautifulSoup(resp.content.decode('utf-8'), "html.parser")
    all_competitions_elements = page.find_all('a', string='на общих основаниях', href=True)
    target_competitions_elements: list[bs4.Tag] = []
    target_competitions_names: list[str] = []
    target_competitions_sub_names: list[str] = []
    for comp_elem in all_competitions_elements:
        cards_levels = comp_elem.find_parents('div', attrs={'class': 'card-body'})
        if len(cards_levels) < 1:
            continue
        form_name_elem = (cards_levels[-1]
                          .parent.find_previous('div').text)
        form_name = form_name_elem.strip()
        if form_name.lower() != 'очная форма обучения':
            continue
        level_name_elem = (cards_levels[-2]
                           .parent.find_previous('div').text)
        level_name = level_name_elem.strip()
        if level_name.lower() != 'магистратура':
            continue
        direction_name_elem = (cards_levels[-3]
                               .parent.find_previous('div'))
        direction_name = direction_name_elem.text.strip()
        sub_direction_name_elem = cards_levels[-4].parent.find_previous('div')
        sub_direction_name = sub_direction_name_elem.text.strip()
        target_competitions_elements.append(comp_elem)
        target_competitions_names.append(direction_name)
        target_competitions_sub_names.append(sub_direction_name)
        pass

    all_competitions_links = [a['href'] for a in target_competitions_elements]

    date_str = datetime.datetime.now().strftime('%d%m%Y-%H%M')
    downloads_folder = pathlib.Path('timacad_downloads')
    if not downloads_folder.exists():
        downloads_folder.mkdir()
    session_folder = downloads_folder / date_str
    if not session_folder.exists():
        session_folder.mkdir()

    async with asyncio.TaskGroup() as tg:
        sem = asyncio.Semaphore(3)
        tasks = []
        progress_bar = tqdm.asyncio.tqdm_asyncio(total=0, desc="Processing")
        for comp_num, comp_link in enumerate(all_competitions_links):
            comp_name = f"{target_competitions_names[comp_num]} ({target_competitions_sub_names[comp_num]})"

            progress_bar.total += 1
            task = tg.create_task(process_comp_link(comp_link, comp_name, session_folder, sem), name=comp_name)
            tasks.append(task)
        for task in asyncio.as_completed(tasks):
            await task
            # progress_bar.write(f"Task {await task} completed")
            progress_bar.update(1)
        # await tqdm.asyncio.tqdm_asyncio.gather(*tasks, desc="Processing", leave=False)
    pass


def main():
    asyncio.run(timacad_crawler())


if __name__ == '__main__':
    main()
