package hamlet.workload;

import hamlet.base.Attribute;
import hamlet.base.DatasetSchema;
import hamlet.users.stockUser.stockAttributeEnum;

public class Generator {
    public static  void  main(String[] args){

        DatasetSchema schema = new DatasetSchema();
        for (stockAttributeEnum a: stockAttributeEnum.values()){
            schema.addAttribute(new Attribute(a.toString()));
        }

            WorkloadTemplate workloadtemplate = new WorkloadTemplate();
            workloadtemplate.generateCandidateQueries( 5);

            for (int i = 20; i<110; i+=10) {
                String workloadFile = "src/main/resources/Revision/Workload_"+i+".txt";
                workloadtemplate.generateWorkload(i, workloadFile);
            }
        }


}
