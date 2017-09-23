package br.com.robsonaraujo;

import java.util.logging.Logger;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class JobLauncherController {
 
    @Autowired
    JobLauncher jobLauncher;
 
    @Autowired
    Job job;
 
    @RequestMapping(value={"/launchjob", "/launchjob/{state}"})
    public String handle(@PathVariable String state) throws Exception {
 
        Logger logger = Logger.getLogger(JobLauncherController.class.getName());
        try {
        	JobParameters jobParameters = new JobParametersBuilder()
        			.addString("timestamp", "" + System.currentTimeMillis())
        			.addString("state", state)
                    .toJobParameters();
            jobLauncher.run(job, jobParameters);
        } catch (Exception e) {
            logger.info(e.getMessage());
        }
 
        return "Done!";
    }
}
