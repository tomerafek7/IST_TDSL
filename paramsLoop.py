import subprocess
import time as t

def dict_to_string(params_dict):
    str = ''
    for param_name, param_value in params_dict.items():
        str += f'{param_name},{param_value}\n'
    return str


if __name__ == '__main__':

    output_file_name = f'results/results.csv'
    num_inner_iterations = 3
    
    numThreads_list = [8]
    numTX_list = [5e6]
    numOpsPerTX_list = [10]
    readRatio_list = [1]
    writeRatio_list = [0]
    deleteRatio_list = [0]
    maxKey_list = [1e8]
    startAmountOfKeys_list = [1e4, 5e4, 1e5, 5e5, 1e6, 2.5e6, 5e6, 7.5e6, 1e7]
    rebuildMinTreeLeafSize_list = [100]
    rebuildCollaborationThreshold_list = [100]
    rebuildUpdatesRatioThreshold_list = [1]
    rebuildMinUpdatesThreshold_list = [50000]
    
    params = {}

    output_file = open(output_file_name, 'w')
    
    loop_count = 0
    for numThreads in numThreads_list:
        for numTX in numTX_list:
            for numOpsPerTX in numOpsPerTX_list:
                for readRatio in readRatio_list:
                    for writeRatio in writeRatio_list:
                        for deleteRatio in deleteRatio_list:
                            for maxKey in maxKey_list:
                                for startAmountOfKeys in startAmountOfKeys_list:
                                    for rebuildMinTreeLeafSize in rebuildMinTreeLeafSize_list:
                                        for rebuildCollaborationThreshold in rebuildCollaborationThreshold_list:
                                            for rebuildUpdatesRatioThreshold in rebuildUpdatesRatioThreshold_list:
                                                for rebuildMinUpdatesThreshold in rebuildMinUpdatesThreshold_list:
                                                    for i in range(num_inner_iterations):
                                                        print(f"begin loop #{loop_count}")
                                                        loop_count += 1
                                                        f = open('config.csv', 'w')
                                                        params['numThreads'] = numThreads
                                                        params['numTX'] = int(numTX)
                                                        params['numOpsPerTX'] = numOpsPerTX
                                                        params['readRatio'] = readRatio
                                                        params['writeRatio'] = writeRatio
                                                        params['deleteRatio'] = deleteRatio
                                                        params['maxKey'] = int(maxKey)
                                                        params['startAmountOfKeys'] = int(startAmountOfKeys)
                                                        params['rebuildMinTreeLeafSize'] = rebuildMinTreeLeafSize
                                                        params['rebuildCollaborationThreshold'] = rebuildCollaborationThreshold
                                                        params['rebuildUpdatesRatioThreshold'] = rebuildUpdatesRatioThreshold
                                                        params['rebuildMinUpdatesThreshold'] = rebuildMinUpdatesThreshold
                                                        params['innerIteration'] = i
                                                        if loop_count == 1:
                                                            output_str = ','.join(list(params.keys()))
                                                            output_str += ',Time,Aborts'
                                                            output_str += '\n'
                                                        config = dict_to_string(params_dict=params)
                                                        output_str += ','.join([str(val) for val in list(params.values())])
                                                        
                                                        f.write(config)
                                                        f.close()
                                                        # now run java program and get output
                                                        # t.sleep(60) # sleep for 1 minute before each iteration
                                                        try: 
                                                            out = subprocess.check_output(['java', '-XX:+UseG1GC', '-cp', 'target/classes:target/test-classes:target/dependency/*', 'ISTWorkLoad', 'config.csv'])
                                                            out_list = ' '.join(out.decode("utf-8").split('\n')).split(' ')
                                                            time = out_list[out_list.index('[ms]')-1]
                                                            # print(out_list)
                                                            aborts = out_list[-2]
                                                            output_str += f',{time},{aborts}'
                                                            output_str += '\n'
                                                        except:
                                                            # skip this iteration if there's an error
                                                            print(f"skipping loop #{loop_count-1} because of an error")
                                                            output_str += '\n' 
                                                        
    output_file.write(output_str)
