import os
import subprocess

from discord.ext.commands import Cog, command

from artifi.discord import Discord,  send_message
from artifi.discord.misc.custom_function import edit_message


class Shell(Cog):
    def __init__(self, bot):
        self._bot: Discord = bot

    @command("shell",
             help="Execute a command directly on the machine shell via the artifi.")
    async def shell_discord(self, ctx, *args):
        if not self._bot.owner_only(ctx):
            return await send_message(ctx, "Access Denied...!")
        elif not args:
            return await send_message(ctx, "Usage: !shell <command>")
        else:
            cmd = ' '.join(args)
            msg = await send_message(ctx, "Running the command...")
            content = "**Shell Out**\n"
            file = None
            try:
                process = subprocess.Popen(
                    cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, encoding='UTF-8', errors='ignore'
                )
                stdout, stderr = process.communicate()

                if stdout or stderr:
                    if len(stdout) + len(stderr) > 2000:
                        # Output is too long, save it to a file
                        with open(os.path.join(self._bot.context.directory, "shell_out.txt"), "w") as f:
                            f.write(f"**** Execution Failed ****\n{stderr}\n\n**** Execution Success ****\n{stdout}")
                        content += "Output is too long, saved to a file."
                        file = os.path.join(self._bot.context.directory, "shell_out.txt")
                    else:
                        if stdout:
                            content += f"```<---Execution Succeed--->\n\n{stdout}```"
                        if stderr:
                            content += f"```<---Execution Failed--->\n\n{stdout}```"
                else:
                    content += f"***Unknown Status***\nExecution May Passed Or Failed..!"
            except Exception as e:
                content += f"***Unable To Execute The Command***\n{str(e)}"
            finally:
                await edit_message(msg, content, files=file)


async def setup(bot):
    await bot.add_cog(Shell(bot))
