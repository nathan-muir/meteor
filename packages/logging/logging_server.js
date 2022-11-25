import chalk from 'chalk';
import { injectPrettify } from "./logging.js";
injectPrettify(function(line, color){
    if(!color) return line;
    return chalk[color](line);
});
export { Log } from './logging.js';
