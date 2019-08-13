class ArcExpr(object):
    _id_counter = 100

    def __init__(self, template, literal=False, subexprs=()):
        self.template = template
        self.subexprs = subexprs
        self.literal = literal
        self.id = 'obj{}'.format(ArcExpr._id_counter)
        ArcExpr._id_counter += 1

    def generate(self):
        queue = [self]
        visited = set()
        let_exprs = []
        while len(queue) > 0:
            head = queue.pop()
            if head not in visited:
                values = [(e.template if e.literal else e.id) for e in head.subexprs]
                code = head.template.format(*values)
                let_exprs.append('let {} = ({});'.format(head.id, code))
                queue.extend(head.subexprs)
                visited.add(head)
        let_exprs.sort()
        return '\n'.join(let_exprs) + '\n' + self.id
