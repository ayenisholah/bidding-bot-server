import { exec, spawn } from 'child_process';
import { promisify } from 'util';
import fs from 'fs/promises';

const execAsync = promisify(exec);

export async function scheduleProductionUpdate(): Promise<{ scheduled: boolean; message: string }> {
  try {
    console.log('Triggering production update...');
    
    // Execute the docker-update script that's included in the container
    const updateProcess = spawn('/usr/local/bin/docker-update', [], {
      detached: true,
      stdio: 'ignore'
    });
    
    // Allow the process to run independently
    updateProcess.unref();
    
    return {
      scheduled: true,
      message: 'Update triggered successfully. Check /tmp/update.log for progress.'
    };
  } catch (error) {
    console.error('Error triggering production update:', error);
    
    // Fallback to direct docker commands if the script isn't available
    try {
      console.log('Attempting direct update approach...');
      
      // Run update commands directly
      const pullCmd = spawn('docker', ['compose', '-f', '/app/compose.yaml', 'pull'], {
        detached: true,
        stdio: 'ignore'
      });
      pullCmd.unref();
      
      // Schedule the recreation after pull completes
      setTimeout(() => {
        const upCmd = spawn('docker', ['compose', '-f', '/app/compose.yaml', 'up', '-d', '--force-recreate'], {
          detached: true,
          stdio: 'ignore'
        });
        upCmd.unref();
      }, 30000); // Wait 30 seconds for pull to complete
      
      return {
        scheduled: true,
        message: 'Update triggered via fallback method. Update will complete in approximately 1 minute.'
      };
    } catch (fallbackError) {
      console.error('Fallback update method also failed:', fallbackError);
      throw fallbackError;
    }
  }
}

export async function getUpdateStatus(): Promise<{ updating: boolean; lastUpdate?: string; logs?: string[] }> {
  try {
    // Read the update log from mounted volume
    const logPath = '/tmp/update.log';
    let logs: string[] = [];
    
    try {
      const logContent = await fs.readFile(logPath, 'utf-8');
      logs = logContent.split('\n').filter(line => line.trim()).slice(-20); // Last 20 lines
    } catch (error) {
      // Log file might not exist yet
      logs = ['No update log found'];
    }
    
    // Check if an update is currently running
    try {
      const { stdout } = await execAsync('ps aux | grep -E "docker.*compose.*(pull|up)" | grep -v grep | wc -l');
      const updating = parseInt(stdout.trim()) > 0;
      
      return {
        updating,
        lastUpdate: logs[logs.length - 1] || 'No updates yet',
        logs
      };
    } catch (error) {
      // If we can't check processes, assume not updating
      return {
        updating: false,
        lastUpdate: logs[logs.length - 1] || 'No updates yet',
        logs
      };
    }
  } catch (error) {
    console.error('Error checking update status:', error);
    return { 
      updating: false, 
      lastUpdate: 'Error checking status',
      logs: ['Error checking update status']
    };
  }
}