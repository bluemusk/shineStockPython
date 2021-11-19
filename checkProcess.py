from datetime import datetime
import psutil

def get_python_process():
    """실행 중인 Python 프로그램 정보를 구한다 """
    rst = [] # 반환값 초기화
    for p in psutil.process_iter():
        list_cmdline_params = p.cmdline() # 커맨드라인정보를 가져온다
        if 'python' == list_cmdline_params[0] : # python 실행인 경우
            program = list_cmdline_params[1] if len(list_cmdline_params) >= 2 else ""
            if program.endswith('.py'): # .py로 끝나는 파일명
                args = list_cmdline_params[2:] if len(list_cmdline_params) >= 3 else [] # Arguments
                py_process_item = dict()
                py_process_item['program'] = program  # 프로그램명
                py_process_item['pid']     = p.pid    # Process ID
                py_process_item['args']    = args     # Arguments
                py_process_item['start_time'] = datetime.fromtimestamp(p.create_time()) # 시작시간
                rst.append(py_process_item)
    return rst

get_python_process()
