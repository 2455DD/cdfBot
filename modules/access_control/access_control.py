from graia.broadcast.builtin.decorators import Depend
from graia.broadcast.exceptions import ExecutionStop
from graia.ariadne.app import Ariadne
from graia.ariadne.model import Group,Member
from graia.ariadne.message.chain import MessageChain
from graia.ariadne.message.element import At

def check_group(*groups: int):
    async def check_group_deco(app: Ariadne, group: Group):
        if group.id not in groups:
            await app.send_message(group, MessageChain("对不起，该群没有该操作权限"))
            raise ExecutionStop
    return Depend(check_group_deco)


def check_member(*members: int):
    async def check_member_deco(app: Ariadne, group: Group, member: Member):
        if member.id not in members:
            await app.send_message(group, MessageChain(At(member.id), "对不起，您的权限并不够"))
            raise ExecutionStop
    return Depend(check_member_deco)
